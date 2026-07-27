import os
import logging
import concurrent.futures
from datetime import datetime, timezone
import sys
from dotenv import load_dotenv
import duckdb

# Dynamically add root project directory so we can import 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.helper_files.lineage import LineageTracker
from src.helper_files.database import DBConnection
from src.helper_files.cloud_utils import get_fs_and_options, inject_duckdb_azure_secret

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silence Azure SDK HTTP Request Spam
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

load_dotenv()

BRONZE_ZONE = os.getenv("BRONZE_ZONE")
SILVER_ZONE = os.getenv("SILVER_ZONE")


def build_transform_query(bronze_path: str, silver_path: str, airflow_run_id: str) -> str:
    """
    Constructs the DuckDB SQL statement to read the Bronze Parquet file,
    cleanse the data, inject V2 audit columns, and write out to the Silver Parquet file.
    """
    bronze_path = bronze_path.replace("\\", "/")
    silver_path = silver_path.replace("\\", "/")

    current_utc = datetime.now(timezone.utc).isoformat()
    airflow_val = f"'{airflow_run_id}'" if airflow_run_id else "NULL"

    return f"""
    COPY (
        WITH base_data AS (
            SELECT
                TRY_CAST(property_id AS BIGINT)     AS property_id,
                TRY_CAST(location_id AS INTEGER)    AS location_id,
                page_url,
                SHA256(page_url)                     AS url_hash,
                property_type,
                TRY_CAST(price AS BIGINT)           AS raw_price,
                location,
                city,
                province_name,
                TRY_CAST(latitude AS DOUBLE)        AS latitude,
                TRY_CAST(longitude AS DOUBLE)       AS longitude,
                TRY_CAST(baths AS INTEGER)          AS baths,
                TRY_CAST(bedrooms AS INTEGER)       AS bedrooms,
                purpose,

                CASE
                    WHEN date_added LIKE '%#%' THEN NULL
                    ELSE COALESCE(
                        TRY_CAST(date_added AS DATE),
                        try_strptime(date_added, '%B %d, %Y')::DATE,
                        try_strptime(date_added, '%m-%d-%Y')::DATE,
                        try_strptime(date_added, '%m/%d/%Y')::DATE
                    )
                END AS date_added,

                COALESCE(NULLIF(TRIM(agency), ''), 'Direct Listing') AS agency,
                COALESCE(NULLIF(TRIM(agent),  ''), 'Not Specified')  AS agent,

                CASE
                    WHEN "Area Type" ILIKE 'Kanal' THEN TRY_CAST("Area Size" AS DOUBLE) * 20.0
                    ELSE TRY_CAST("Area Size" AS DOUBLE)
                END AS area_marla,

                _ingested_at,
                _airflow_run_id AS _bronze_airflow_run_id

            FROM read_parquet('{bronze_path}')
        )
        SELECT
            property_id,
            location_id,
            url_hash,
            page_url,
            property_type,

            CASE
                WHEN raw_price > 500000000 THEN 500000000
                ELSE raw_price
            END AS price,

            CASE
                WHEN area_marla > 0 THEN raw_price / area_marla
                ELSE NULL
            END AS price_per_marla,

            location,
            city,
            province_name,

            CASE WHEN latitude  BETWEEN 24.0 AND 37.0 THEN latitude  ELSE NULL END AS latitude,
            CASE WHEN longitude BETWEEN 61.0 AND 78.0 THEN longitude ELSE NULL END AS longitude,

            baths,
            bedrooms,
            purpose,
            date_added,
            agency,
            agent,
            area_marla,

            _ingested_at,
            _bronze_airflow_run_id,
            '{current_utc}' AS _transformed_at,
            {airflow_val}   AS _silver_airflow_run_id

        FROM base_data
    ) TO '{silver_path}' (FORMAT PARQUET);
    """


def get_eligible_bronze_files():
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT file_hash, file_name FROM file_lineage WHERE layer='BRONZE' AND status='SUCCESS'")
            return cur.fetchall()


def process_silver_task(file_hash: str, bronze_filename: str, silver_fs, airflow_run_id: str):
    """
    Worker task that initializes an independent thread-local DuckDB instance
    and transforms a single file directly over the network.
    """
    silver_filename = bronze_filename.replace(".parquet", "_clean.parquet")
    bronze_path = f"{BRONZE_ZONE}/{bronze_filename}"
    silver_path = f"{SILVER_ZONE}/{silver_filename}"

    # Thread-Isolated DuckDB Instance
    duckdb_conn = duckdb.connect(database=':memory:')
    duckdb_conn.execute("PRAGMA memory_limit='1GB';")

    try:
        # Bypass DuckDB's C++ Azure extension SSL bug by registering the rock-solid Python fsspec filesystem
        if SILVER_ZONE.startswith("abfs://") or BRONZE_ZONE.startswith("abfs://"):
            duckdb_conn.register_filesystem(silver_fs)
            
        local_tmp_path = f"/tmp/{silver_filename}"
        
        # 1. DuckDB streams read from Azure but writes to lightning-fast local SSD
        query = build_transform_query(bronze_path, local_tmp_path, airflow_run_id)
        duckdb_conn.execute(query)

        # 2. Upload the fully formed file to Azure in one solid chunk (prevents fsspec socket timeouts)
        if SILVER_ZONE.startswith("abfs://"):
            silver_fs.put_file(local_tmp_path, silver_path)
        else:
            silver_fs.put_file(local_tmp_path, silver_path) # Works for local FS too

        # 3. Get metrics and clean up
        file_size = os.path.getsize(local_tmp_path)
        
        escaped_local = local_tmp_path.replace("\\", "/")
        row_count = duckdb_conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{escaped_local}')"
        ).fetchone()[0]

        try:
            os.remove(local_tmp_path)
        except OSError:
            pass

        return {
            "status": "SUCCESS",
            "file_hash": file_hash,
            "silver_filename": silver_filename,
            "row_count": row_count,
            "file_size": file_size
        }
    except Exception as e:
        error_msg = str(e)[:500]
        # Clean up partial writes if they exist
        try:
            if silver_fs.exists(silver_path):
                silver_fs.rm(silver_path)
        except:
            pass
            
        return {
            "status": "FAILED",
            "file_hash": file_hash,
            "silver_filename": silver_filename,
            "error_msg": error_msg
        }
    finally:
        duckdb_conn.close()


def transform_to_silver(batch_limit: int = 0, airflow_run_id: str = None):
    if not BRONZE_ZONE or not SILVER_ZONE:
        logging.error("BRONZE_ZONE or SILVER_ZONE is not set in the .env file. Aborting.")
        return

    bronze_fs, _ = get_fs_and_options(BRONZE_ZONE)
    silver_fs, _ = get_fs_and_options(SILVER_ZONE)

    silver_fs.makedirs(SILVER_ZONE, exist_ok=True)

    eligible_files = get_eligible_bronze_files()
    logging.info(f"Found {len(eligible_files)} Parquet file(s) eligible for Silver transformation.")

    if not eligible_files:
        logging.info("Nothing to transform. Exiting.")
        return

    processed_count = 0

    with LineageTracker("SILVER", airflow_run_id=airflow_run_id) as tracker:
        files_to_process = []
        
        for file_hash, bronze_filename in eligible_files:
            if tracker.is_file_processed(file_hash):
                logging.info(f"SKIP: {bronze_filename} (already processed).")
                continue

            bronze_path = f"{BRONZE_ZONE}/{bronze_filename}"
            if not bronze_fs.exists(bronze_path):
                logging.warning(f"File missing on disk/cloud: {bronze_path}. Skipping.")
                continue

            files_to_process.append((file_hash, bronze_filename))
            if batch_limit > 0 and len(files_to_process) >= batch_limit:
                logging.info(f"Batch limit of {batch_limit} reached.")
                break

        dynamic_threads = min(len(files_to_process), 10)
        
        if dynamic_threads > 0:
            logging.info(f"⚙️ Dynamic Threading: Initializing {dynamic_threads} independent DuckDB workers for {len(files_to_process)} files.")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=dynamic_threads) as executor:
                future_to_file = {
                    executor.submit(process_silver_task, fh, bfn, silver_fs, airflow_run_id): bfn 
                    for fh, bfn in files_to_process
                }
                
                for future in concurrent.futures.as_completed(future_to_file):
                    res = future.result()
                    
                    if res["status"] == "SUCCESS":
                        logging.info(f"✅ SUCCESS: {res['silver_filename']} ({res['row_count']:,} rows, {res['file_size'] / 1e6:.1f} MB)")
                        tracker.log_result(
                            file_hash=res["file_hash"],
                            file_name=res["silver_filename"],
                            status="SUCCESS",
                            row_count=res["row_count"],
                            file_size_bytes=res["file_size"]
                        )
                        processed_count += 1
                    elif res["status"] == "FAILED":
                        logging.error(f"❌ FAILED: {res['silver_filename']} - {res['error_msg']}")
                        tracker.log_result(
                            file_hash=res["file_hash"],
                            file_name=res["silver_filename"],
                            status="FAILED",
                            error_message=res["error_msg"]
                        )

    logging.info(f"🎉 Silver transformation complete. {processed_count}/{len(files_to_process)} file(s) transformed successfully.")


if __name__ == "__main__":
    run_id = os.getenv("AIRFLOW_RUN_ID")
    transform_to_silver(airflow_run_id=run_id)
