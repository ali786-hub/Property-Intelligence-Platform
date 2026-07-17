import os
import glob
import hashlib
import logging
import polars as pl
from dotenv import load_dotenv
from src.helper_files.lineage import LineageTracker

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load directory configurations
load_dotenv()
LANDING_ZONE = os.getenv("LANDING_ZONE")
BRONZE_ZONE = os.getenv("BRONZE_ZONE")

# Ensure the output directory exists
if BRONZE_ZONE:
    os.makedirs(BRONZE_ZONE, exist_ok=True)


def calculate_file_hash(file_path: str) -> str:
    """
    Computes the SHA-256 hash of a file using chunked streaming
    to keep the memory footprint extremely low.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as file:
        while True:
            chunk = file.read(8192)  # Read in 8KB chunks
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def ingest_to_bronze(batch_limit: int = 0):
    """
    Scans the landing zone for CSV files, checks if they have been
    previously processed using the LineageTracker, streams them to 
    Parquet format using Polars, and updates the database lineage.
    """
    if not LANDING_ZONE or not BRONZE_ZONE:
        logging.error("LANDING_ZONE or BRONZE_ZONE not set in environment.")
        return

    # 1. Initialize our lineage tracker for the BRONZE layer
    # The context manager ensures the tracker always flushes its buffer on exit
    with LineageTracker('BRONZE') as tracker:
        
        # 2. Scan the landing folder for CSV files using glob
        search_path = os.path.join(LANDING_ZONE, "*.csv")
        csv_files = glob.glob(search_path)
        
        logging.info(f"Found {len(csv_files)} CSV file(s) in the landing zone.")
        
        processed_count = 0
        
        for file_path in csv_files:
            # Handle batch limit constraint
            if batch_limit > 0 and processed_count >= batch_limit:
                logging.info(f"Batch limit of {batch_limit} reached. Stopping ingestion.")
                break
                
            file_name = os.path.basename(file_path)
            
            try:
                # 3. Calculate SHA-256 hash (DNA of the file)
                file_hash = calculate_file_hash(file_path)
                
                # 4. Check if the file is already processed
                if tracker.is_file_processed(file_hash):
                    logging.info(f"Skipping {file_name} (already processed successfully).")
                    continue
                
                logging.info(f"Processing {file_name}...")
                
                # Define output path in the Bronze zone
                parquet_name = file_name.replace(".csv", ".parquet")
                output_path = os.path.join(BRONZE_ZONE, parquet_name)
                
                # 5. Convert CSV to Parquet using Polars streaming engine (OOM-Safe)
                lf = pl.scan_csv(file_path, ignore_errors=True)
                lf.sink_parquet(output_path)
                
                # 6. Retrieve metrics of the processed file
                file_size = os.path.getsize(output_path)
                
                # Polars can read Parquet metadata instantly to get row counts (0ms compute)
                row_count = pl.scan_parquet(output_path).select(pl.len()).collect().item()
                
                # 7. Log success to the RAM buffer
                tracker.log_result(
                    file_hash=file_hash,
                    file_name=parquet_name,
                    status='SUCCESS',
                    row_count=row_count,
                    file_size_bytes=file_size
                )
                
                # 8. Delete raw CSV from landing zone (Clean architecture)
                os.remove(file_path)
                logging.info(f"Successfully ingested {file_name} -> {parquet_name} ({row_count:,} rows)")
                processed_count += 1
                
            except Exception as e:
                # Log failure details to the RAM buffer so they reach our audit DB
                error_msg = str(e)
                logging.error(f"Failed to process {file_name}: {error_msg}")
                try:
                    file_hash = calculate_file_hash(file_path)
                    tracker.log_result(
                        file_hash=file_hash,
                        file_name=file_name,
                        status='FAILED',
                        error_message=error_msg
                    )
                except Exception as inner_err:
                    logging.error(f"Could not log failure for {file_name} to database: {inner_err}")


if __name__ == "__main__":
    # For local execution testing, process all files
    ingest_to_bronze()
