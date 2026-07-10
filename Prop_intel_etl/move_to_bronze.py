"""
PropIntel Bronze Layer ETL.

Responsibility: Convert raw CSVs from landing_zone into Parquet files in
the bronze zone. 1-to-1 mapping — no data is changed, only the format.

All Neon DB state tracking is handled by PipelineTracker.
This script only needs to think about: find files → convert → archive.

Usage:
    python move_to_bronze.py            # Process all unprocessed files
    python move_to_bronze.py --batch 3  # Process only 3 files
"""

import os
import glob
import hashlib
import shutil
import argparse
import polars as pl

from pipeline_tracker import PipelineTracker

# ---------------------------------------------------------------
# PATH CONFIGURATION
# Derived relative to this script's location — no hardcoded paths.
# Script lives in: PropIntel/Prop_intel_etl/
# Workspace root:  PropIntel/../  (one level up from PropI)
# ---------------------------------------------------------------
SCRIPT_DIR     = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT   = os.path.dirname(SCRIPT_DIR)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

LANDING_ZONE = os.path.join(WORKSPACE_ROOT, "data", "landing_zone")
ARCHIVE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "archive_zone")
BRONZE_ZONE  = os.path.join(WORKSPACE_ROOT, "data", "bronze")

os.makedirs(LANDING_ZONE, exist_ok=True)
os.makedirs(ARCHIVE_ZONE, exist_ok=True)
os.makedirs(BRONZE_ZONE,  exist_ok=True)


def get_md5(file_path: str) -> str:
    """
    Calculates MD5 hash of a file using chunked reading.
    Chunked reading is critical for 50MB+ files — reading the whole
    file into memory at once would spike RAM usage significantly.
    """
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()


def main():
    parser = argparse.ArgumentParser(description="PropIntel Bronze Layer ETL")
    parser.add_argument("--batch", type=int, default=0,
                        help="Number of files to process. 0 = all unprocessed (default).")
    args = parser.parse_args()

    # Scan landing zone for all CSVs
    all_csvs = sorted(glob.glob(os.path.join(LANDING_ZONE, "*.csv")))
    if not all_csvs:
        print("Landing zone is empty. Nothing to process.")
        return?  

    print(f"Found {len(all_csvs)} CSV(s) in landing zone.")

    # Use the tracker as a context manager.
    # __enter__ connects to Neon and starts a run.
    # __exit__ finalizes the run and closes the connection (even if we crash).
    with PipelineTracker('BRONZE') as tracker:

        # SINGLE QUERY: Get all hashes already successfully processed.
        # Previously this was one query PER FILE — now it's one total.
        processed_hashes   = tracker.get_processed_hashes()
        quarantined_hashes = tracker.get_quarantined_hashes()

        # Filter locally in Python using O(1) set lookups
        unprocessed = [
            (path, get_md5(path))
            for path in all_csvs
            if get_md5(path) not in processed_hashes
            and get_md5(path) not in quarantined_hashes
        ]

        # Avoid computing MD5 three times — recalculate cleanly
        unprocessed = []
        for path in all_csvs:
            h = get_md5(path)
            if h not in processed_hashes and h not in quarantined_hashes:
                unprocessed.append((path, h))

        total = len(unprocessed)
        print(f"{total} file(s) need processing "
              f"({len(processed_hashes)} already done, "
              f"{len(quarantined_hashes)} quarantined).")

        if total == 0:
            print("Nothing to do. Exiting.")
            return

        # Apply batch limit
        batch_size = args.batch if args.batch > 0 else total
        batch_size = min(batch_size, total)
        target_batch = unprocessed[:batch_size]
        print(f"Processing {batch_size} file(s)...\n")

        for file_path, file_hash in target_batch:
            filename         = os.path.basename(file_path)
            parquet_filename = filename.replace(".csv", ".parquet")
            output_path      = os.path.join(BRONZE_ZONE, parquet_filename)

            # Pre-log: crash-safe. If we die here, Neon records PROCESSING
            # and the file will be retried on the next run (up to MAX_RETRIES).
            tracker.pre_log(file_hash, parquet_filename)

            try:
                # Read CSV (lazy + ignore_errors handles malformed rows gracefully)
                df = pl.scan_csv(file_path, ignore_errors=True).collect()
                row_count = df.height

                # Write Parquet
                df.write_parquet(output_path)

                # Verify the write succeeded by checking output size
                file_size = os.path.getsize(output_path)
                if file_size == 0:
                    raise ValueError(f"Output Parquet file is 0 bytes — write failed silently.")

                # Archive the original CSV (move, not copy)
                shutil.move(file_path, os.path.join(ARCHIVE_ZONE, filename))

                tracker.log_success(file_hash, row_count, file_size)
                print(f"  [OK] {parquet_filename} ({row_count:,} rows, {file_size / 1e6:.1f} MB)")

            except Exception as e:
                error_msg = str(e)
                print(f"  [FAIL] {filename}: {error_msg}")
                tracker.log_failure(file_hash, error_msg)


if __name__ == "__main__":
    main()