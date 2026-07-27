import os
import fsspec
import hashlib
import logging
import polars as pl
import sys
import concurrent.futures
from datetime import datetime, timezone
from dotenv import load_dotenv

# Dynamically add root project directory so we can import 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.helper_files.lineage import LineageTracker
from src.helper_files.cloud_utils import get_fs_and_options

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Silence Azure SDK HTTP Request Spam
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
load_dotenv()

LANDING_ZONE = os.getenv("LANDING_ZONE")
BRONZE_ZONE = os.getenv("BRONZE_ZONE")
QUARANTINE_ZONE = os.getenv("QUARANTINE_ZONE", f"{BRONZE_ZONE}/quarantine" if BRONZE_ZONE else None)


def process_file_task(file_path: str, landing_fs, bronze_fs, bronze_opts, airflow_run_id: str, tracker_processed_hashes: set):
    """
    Worker task that downloads a single file to /tmp, hashes it locally,
    processes it with Polars, uploads it to Bronze, and returns a result dict.
    """
    file_name = file_path.split("/")[-1]
    local_temp_csv = f"/tmp/{file_name}"
    local_temp_parquet = f"/tmp/{file_name.replace('.csv', '.parquet')}"
    parquet_name = file_name.replace(".csv", ".parquet")
    output_path = f"{BRONZE_ZONE}/{parquet_name}"
    
    file_hash = "UNKNOWN_HASH"
    
    try:
        # 1. High-Speed Local Download
        landing_fs.get(file_path, local_temp_csv)
        
        # 2. Instant Local Hashing (SSD speeds)
        sha256 = hashlib.sha256()
        with open(local_temp_csv, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk: break
                sha256.update(chunk)
        file_hash = sha256.hexdigest()
        
        # 3. Idempotency Check (Skip if already processed)
        if file_hash in tracker_processed_hashes:
            landing_fs.rm(file_path)
            if os.path.exists(local_temp_csv): os.remove(local_temp_csv)
            return {"status": "SKIPPED", "file_name": file_name, "message": "Already processed"}

        # 4. Process with Polars Locally
        lf = pl.scan_csv(local_temp_csv, ignore_errors=True, infer_schema_length=0)
        lf = lf.with_columns(
            pl.lit(datetime.now(timezone.utc).isoformat()).alias("_ingested_at"),
            pl.lit(airflow_run_id).alias("_airflow_run_id"),
        )
        df = lf.collect()
        
        # Write local parquet
        df.write_parquet(local_temp_parquet)
        
        # 5. Upload Parquet to Bronze
        bronze_fs.put(local_temp_parquet, output_path)
        
        row_count = len(df)
        file_size = bronze_fs.info(output_path)["size"]
        
        # 6. Cleanup remote landing zone and local tmp
        landing_fs.rm(file_path)
        if os.path.exists(local_temp_csv): os.remove(local_temp_csv)
        if os.path.exists(local_temp_parquet): os.remove(local_temp_parquet)
        
        return {
            "status": "SUCCESS", 
            "file_name": parquet_name, 
            "file_hash": file_hash, 
            "row_count": row_count, 
            "file_size": file_size
        }

    except Exception as e:
        error_msg = str(e)[:500]
        
        # Move to quarantine
        quarantine_dest = f"{QUARANTINE_ZONE}/{file_name}"
        try:
            landing_fs.copy(file_path, quarantine_dest)
            landing_fs.rm(file_path)
        except Exception as move_err:
            logging.error(f"Could not move {file_name} to quarantine: {move_err}")
            
        # Cleanup
        if os.path.exists(local_temp_csv): os.remove(local_temp_csv)
        if os.path.exists(local_temp_parquet): os.remove(local_temp_parquet)
            
        return {
            "status": "FAILED", 
            "file_name": file_name, 
            "file_hash": file_hash, 
            "error_msg": error_msg
        }


def ingest_to_bronze(batch_limit: int = 0, airflow_run_id: str = None):
    if not LANDING_ZONE or not BRONZE_ZONE:
        logging.error("LANDING_ZONE or BRONZE_ZONE is not set in the .env file. Aborting.")
        return

    landing_fs, _ = get_fs_and_options(LANDING_ZONE)
    bronze_fs, bronze_opts = get_fs_and_options(BRONZE_ZONE)

    bronze_fs.makedirs(BRONZE_ZONE, exist_ok=True)
    bronze_fs.makedirs(QUARANTINE_ZONE, exist_ok=True)

    search_pattern = f"{LANDING_ZONE}/*.csv".replace("\\", "/")
    csv_files = landing_fs.glob(search_pattern)
    
    logging.info(f"Found {len(csv_files)} CSV file(s) in the landing zone.")

    if not csv_files:
        logging.info("Nothing to ingest. Exiting.")
        return

    # Prepare file paths
    files_to_process = []
    for file_path in csv_files:
        if LANDING_ZONE.startswith("abfs://") and not file_path.startswith("abfs://"):
            file_path = f"abfs://{file_path}"
        files_to_process.append(file_path)
        
    if batch_limit > 0:
        files_to_process = files_to_process[:batch_limit]

    with LineageTracker("BRONZE", airflow_run_id=airflow_run_id) as tracker:
        processed_hashes = tracker.processed_hashes
        
        dynamic_threads = min(len(files_to_process), 10)
        logging.info(f"⚙️ Dynamic Threading: Initializing {dynamic_threads} workers for {len(files_to_process)} files.")
        
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=dynamic_threads) as executor:
            future_to_file = {
                executor.submit(process_file_task, f, landing_fs, bronze_fs, bronze_opts, airflow_run_id, processed_hashes): f 
                for f in files_to_process
            }
            
            for future in concurrent.futures.as_completed(future_to_file):
                res = future.result()
                
                if res["status"] == "SUCCESS":
                    logging.info(f"✅ SUCCESS: {res['file_name']} ({res['row_count']:,} rows, {res['file_size']:,} bytes)")
                    tracker.log_result(
                        file_hash=res["file_hash"],
                        file_name=res["file_name"],
                        status="SUCCESS",
                        row_count=res["row_count"],
                        file_size_bytes=res["file_size"]
                    )
                elif res["status"] == "FAILED":
                    logging.error(f"❌ FAILED: {res['file_name']} - {res['error_msg']}")
                    tracker.log_result(
                        file_hash=res.get("file_hash", "UNKNOWN_HASH"),
                        file_name=res["file_name"],
                        status="FAILED",
                        error_message=res["error_msg"]
                    )
                else:
                    logging.info(f"⏭️ SKIP: {res['file_name']} ({res['message']})")

    logging.info("🎉 Bronze ingestion complete.")


if __name__ == "__main__":
    run_id = os.getenv("AIRFLOW_RUN_ID")
    ingest_to_bronze(airflow_run_id=run_id)