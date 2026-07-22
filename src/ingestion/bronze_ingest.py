import os
import glob
import shutil
import hashlib
import logging
import polars as pl
from datetime import datetime, timezone
from dotenv import load_dotenv
from src.helper_files.lineage import LineageTracker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

LANDING_ZONE = os.getenv("LANDING_ZONE")
BRONZE_ZONE = os.getenv("BRONZE_ZONE")
QUARANTINE_ZONE = os.getenv("QUARANTINE_ZONE", os.path.join(BRONZE_ZONE or "", "quarantine"))


def calculate_file_hash(file_path: str) -> str:
    """Reads a file in 8KB chunks and returns its SHA-256 hex digest."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def ingest_to_bronze(batch_limit: int = 0, airflow_run_id: str = None):
    """
    Scans the landing zone for raw CSV files, converts each to Parquet
    in the Bronze layer using Polars streaming, and logs every result
    (success or failure) to PostgreSQL via the LineageTracker.

    Args:
        batch_limit:    Max files to process in this run. 0 = unlimited.
        airflow_run_id: Passed by the Airflow DAG so every record is traceable.
    """
    if not LANDING_ZONE or not BRONZE_ZONE:
        logging.error("LANDING_ZONE or BRONZE_ZONE is not set in the .env file. Aborting.")
        return

    # Ensure output directories exist
    os.makedirs(BRONZE_ZONE, exist_ok=True)
    os.makedirs(QUARANTINE_ZONE, exist_ok=True)

    # Scan the landing zone for CSV files
    csv_files = glob.glob(os.path.join(LANDING_ZONE, "*.csv"))
    logging.info(f"Found {len(csv_files)} CSV file(s) in the landing zone.")

    if not csv_files:
        logging.info("Nothing to ingest. Exiting.")
        return

    processed_count = 0

    with LineageTracker("BRONZE", airflow_run_id=airflow_run_id) as tracker:

        for file_path in csv_files:

            # Respect the batch limit if set
            if batch_limit > 0 and processed_count >= batch_limit:
                logging.info(f"Batch limit of {batch_limit} reached. Stopping.")
                break

            file_name = os.path.basename(file_path)

            # Step 1: Calculate the SHA-256 hash (the file's unique DNA)
            file_hash = calculate_file_hash(file_path)

            # Step 2: Check the in-memory cache — skip if already SUCCESS
            if tracker.is_file_processed(file_hash):
                logging.info(f"SKIP: {file_name} (already processed).")
                continue

            # Step 3: Attempt the CSV → Parquet conversion
            parquet_name = file_name.replace(".csv", ".parquet")
            output_path = os.path.join(BRONZE_ZONE, parquet_name)

            try:
                # Polars LazyFrame: reads the CSV in streaming chunks, never loads
                # the entire file into RAM, and writes directly to Parquet on disk.
                lf = pl.scan_csv(file_path, ignore_errors=True)

                # Inject audit metadata columns so every row knows where it came from
                lf = lf.with_columns(
                    pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingested_at"),
                    pl.lit(airflow_run_id).alias("_airflow_run_id"),
                )

                # Stream directly to disk — OOM-safe regardless of file size
                lf.sink_parquet(output_path)

                # Collect output metrics from the Parquet metadata (instant, no re-read)
                row_count = pl.scan_parquet(output_path).select(pl.len()).collect().item()
                file_size = os.path.getsize(output_path)

                # Log SUCCESS to the lineage tracker buffer
                tracker.log_result(
                    file_hash=file_hash,
                    file_name=parquet_name,
                    status="SUCCESS",
                    row_count=row_count,
                    file_size_bytes=file_size,
                )

                # Delete the raw CSV from the landing zone (it is now safely in Bronze)
                os.remove(file_path)
                processed_count += 1
                logging.info(f"SUCCESS: {file_name} -> {parquet_name} ({row_count:,} rows, {file_size:,} bytes)")

            except Exception as e:
                error_msg = str(e)[:500]  # Truncate to avoid bloating the DB
                logging.error(f"FAILED: {file_name} — {error_msg}")

                # Log FAILED to the lineage tracker buffer
                tracker.log_result(
                    file_hash=file_hash,
                    file_name=file_name,
                    status="FAILED",
                    error_message=error_msg,
                )

                # Move the corrupt file to quarantine so it doesn't block future runs
                quarantine_dest = os.path.join(QUARANTINE_ZONE, file_name)
                try:
                    shutil.move(file_path, quarantine_dest)
                    logging.info(f"QUARANTINED: {file_name} moved to {QUARANTINE_ZONE}")
                except Exception as move_err:
                    logging.error(f"Could not move {file_name} to quarantine: {move_err}")

                # Clean up any partial Parquet file that may have been written before the crash
                if os.path.exists(output_path):
                    os.remove(output_path)

    logging.info(f"Bronze ingestion complete. {processed_count}/{len(csv_files)} file(s) processed successfully.")


if __name__ == "__main__":
    ingest_to_bronze()
