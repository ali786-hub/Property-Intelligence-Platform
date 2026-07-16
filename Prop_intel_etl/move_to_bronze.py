"""
PropIntel Bronze Layer ETL.

Ingests raw CSV files from the landing zone, calculates their hashes to ensure 
idempotency, and converts them to Parquet format in the bronze zone.
"""

import os
import glob
import hashlib
import shutil
import argparse
import polars as pl

from pipeline_tracker import PipelineTracker

# --- Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

LANDING_ZONE = os.path.join(WORKSPACE_ROOT, "data", "landing_zone")
ARCHIVE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "archive_zone")
BRONZE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "bronze")

os.makedirs(LANDING_ZONE, exist_ok=True)
os.makedirs(ARCHIVE_ZONE, exist_ok=True)
os.makedirs(BRONZE_ZONE, exist_ok=True)


def calculate_file_hash(file_path: str) -> str:
    """
    Computes the MD5 hash of a file using chunked streaming.
    
    Args:
        file_path: Path to the target file.
    Returns:
        The MD5 hex string of the file contents.
    """
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()


def main():
    parser = argparse.ArgumentParser(description="PropIntel Bronze Layer Ingestion")
    parser.add_argument("--batch", type=int, default=0,
                        help="Maximum number of files to process. 0 = process all (default).")
    args = parser.parse_args()

    # Discover and sort landing zone CSVs
    raw_files = sorted(glob.glob(os.path.join(LANDING_ZONE, "*.csv")))
    if not raw_files:
        print("Landing zone is empty. No files to process.")
        return

    print(f"Found {len(raw_files)} raw CSV file(s) in landing zone.")

    with PipelineTracker('BRONZE') as tracker:
        processed_hashes = tracker.get_processed_hashes()
        quarantined_hashes = tracker.get_quarantined_hashes()

        # Identify files that haven't been successfully processed or quarantined
        unprocessed = []
        for file_path in raw_files:
            file_hash = calculate_file_hash(file_path)
            if file_hash not in processed_hashes and file_hash not in quarantined_hashes:
                unprocessed.append((file_path, file_hash))

        total_files = len(unprocessed)
        print(f"{total_files} file(s) require processing "
              f"({len(processed_hashes)} processed, {len(quarantined_hashes)} quarantined).")

        if total_files == 0:
            print("Nothing to process.")
            return

        # Determine target batch size
        batch_limit = args.batch if args.batch > 0 else total_files
        batch_size = min(batch_limit, total_files)
        target_files = unprocessed[:batch_size]
        print(f"Processing batch of {batch_size} file(s)...\n")

        for file_path, file_hash in target_files:
            base_name = os.path.basename(file_path)
            parquet_name = base_name.replace(".csv", ".parquet")
            output_path = os.path.join(BRONZE_ZONE, parquet_name)

            # Pre-log the file state to Neon database
            tracker.pre_log(file_hash, parquet_name)

            try:
                # Read CSV via Polars and collect into memory (ignoring malformed rows)
                df = pl.scan_csv(file_path, ignore_errors=True).collect()
                row_count = df.height

                # Write to Parquet format
                df.write_parquet(output_path)

                # Validate output file integrity
                file_size = os.path.getsize(output_path)
                if file_size == 0:
                    raise ValueError("Output Parquet file is empty.")

                # Move original CSV to archive zone on success
                shutil.move(file_path, os.path.join(ARCHIVE_ZONE, base_name))

                # Update lineage state to SUCCESS
                tracker.log_success(file_hash, row_count, file_size)
                print(f"  [OK] {parquet_name} ({row_count:,} rows, {file_size / 1e6:.1f} MB)")

            except Exception as e:
                error_msg = str(e)
                print(f"  [FAIL] {base_name}: {error_msg}")
                tracker.log_failure(file_hash, error_msg)


if __name__ == "__main__":
    main()