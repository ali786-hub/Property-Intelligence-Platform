"""
PropIntel Bronze Layer ETL.

Scans the landing zone for new CSV files, converts them to Parquet (1-to-1),
logs lineage to Neon DB, and archives the originals. Idempotent via MD5 hashing.

Usage:
    python move_to_bronze.py            # Process all unprocessed files
    python move_to_bronze.py --batch 3  # Process only 3 files
"""

import os
import glob
import hashlib
import shutil
import argparse
import psycopg2
import polars as pl
from dotenv import load_dotenv

# Load variables from the .env file
load_dotenv()

# --- Configuration ---
NEON_CONN_STR = os.getenv("DATABASE_URL")
if not NEON_CONN_STR:
    raise ValueError("CRITICAL ERROR: Python cannot find DATABASE_URL. Check your .env file!")

# Derive all paths relative to this script's location.
# Script lives in: PropI/Prop_intel_etl/
# Workspace root is two levels up: PropI/ -> Omnijourney_Kofking_github/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

LANDING_ZONE = os.path.join(WORKSPACE_ROOT, "data", "landing_zone")
ARCHIVE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "archive_zone")
BRONZE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "bronze")

# Ensure directories exist
os.makedirs(LANDING_ZONE, exist_ok=True)
os.makedirs(ARCHIVE_ZONE, exist_ok=True)
os.makedirs(BRONZE_ZONE, exist_ok=True)


def get_md5(file_path):
    """Calculates the MD5 hash of a file using chunked reading (memory safe for large files)."""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def main():
    # --- CLI Arguments ---
    parser = argparse.ArgumentParser(description="PropIntel Bronze Layer ETL")
    parser.add_argument("--batch", type=int, default=0,
                        help="Number of files to process. 0 = all unprocessed files (default).")
    args = parser.parse_args()

    # 1. Connect to Neon
    print("Connecting to Pipeline Brain (Neon)...")
    conn = psycopg2.connect(NEON_CONN_STR)
    cur = conn.cursor()

    # 2. Start the Run
    cur.execute(
        "INSERT INTO pipeline_runs (status) VALUES ('RUNNING') RETURNING run_id;"
    )
    run_id = cur.fetchone()[0]
    conn.commit()
    print(f"Started Run ID: {run_id}")

    # 3. Scan & Filter (The Intelligence)
    all_csvs = sorted(glob.glob(os.path.join(LANDING_ZONE, "*.csv")))
    if not all_csvs:
        print("Landing zone is empty. Nothing to process.")
        cur.execute("UPDATE pipeline_runs SET status = 'SUCCESS', ended_at = CURRENT_TIMESTAMP WHERE run_id = %s;", (run_id,))
        conn.commit()
        cur.close()
        conn.close()
        return

    unprocessed_files = []

    for file_path in all_csvs:
        file_hash = get_md5(file_path)

        # Ask Neon if this hash already succeeded in Bronze
        cur.execute("""
            SELECT 1 FROM file_lineage 
            WHERE file_hash = %s AND layer = 'BRONZE' AND status = 'SUCCESS';
        """, (file_hash,))

        if not cur.fetchone():
            unprocessed_files.append((file_path, file_hash))

    total_unprocessed = len(unprocessed_files)
    print(f"Found {len(all_csvs)} total files. {total_unprocessed} need processing.")

    if total_unprocessed == 0:
        print("All files already processed. Exiting.")
        cur.execute("UPDATE pipeline_runs SET status = 'SUCCESS', ended_at = CURRENT_TIMESTAMP WHERE run_id = %s;", (run_id,))
        conn.commit()
        cur.close()
        conn.close()
        return

    # 4. Determine batch size from CLI argument
    batch_size = args.batch if args.batch > 0 else total_unprocessed
    batch_size = min(batch_size, total_unprocessed)
    target_batch = unprocessed_files[:batch_size]

    # 5. Pre-log to Database
    for file_path, file_hash in target_batch:
        filename = os.path.basename(file_path)
        cur.execute("""
            INSERT INTO file_lineage (file_hash, layer, file_name, run_id, status)
            VALUES (%s, 'BRONZE', %s, %s, 'PROCESSING')
            ON CONFLICT (file_hash, layer) 
            DO UPDATE SET status = 'PROCESSING', run_id = EXCLUDED.run_id, updated_at = CURRENT_TIMESTAMP;
        """, (file_hash, filename, run_id))
    conn.commit()

    # 6. Polars Execution (1-to-1 CSV -> Parquet)
    print(f"Processing {len(target_batch)} files...")
    success_count = 0
    fail_count = 0

    for file_path, file_hash in target_batch:
        filename = os.path.basename(file_path)
        parquet_filename = filename.replace(".csv", ".parquet")
        output_file = os.path.join(BRONZE_ZONE, parquet_filename)

        try:
            # Read ONE CSV, convert to Parquet
            df = pl.scan_csv(file_path, ignore_errors=True).collect()
            row_count = df.height

            df.write_parquet(output_file)
            print(f"  [OK] {parquet_filename} ({row_count:,} rows)")

            # Post-Log Success & Archive
            cur.execute("""
                UPDATE file_lineage 
                SET status = 'SUCCESS', row_count = %s, updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s AND layer = 'BRONZE';
            """, (row_count, file_hash))

            shutil.move(file_path, os.path.join(ARCHIVE_ZONE, filename))
            success_count += 1

        except Exception as e:
            # Per-file error handling — one bad file does NOT kill the batch
            error_msg = str(e)
            print(f"  [FAIL] {filename}: {error_msg}")

            cur.execute("""
                UPDATE file_lineage 
                SET status = 'FAILED', error_message = %s, updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s AND layer = 'BRONZE';
            """, (error_msg, file_hash))
            fail_count += 1

    # 7. Finalize Run
    final_status = 'SUCCESS' if fail_count == 0 else 'PARTIAL_SUCCESS'
    cur.execute("UPDATE pipeline_runs SET status = %s, ended_at = CURRENT_TIMESTAMP WHERE run_id = %s;",
                (final_status, run_id))
    conn.commit()

    print(f"\nBronze batch complete: {success_count} succeeded, {fail_count} failed.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()