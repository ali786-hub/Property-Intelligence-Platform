import os
import glob
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import duckdb

from src.helper_files.lineage import LineageTracker
from src.helper_files.database import DBConnection

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

BRONZE_ZONE = os.getenv("BRONZE_ZONE")
SILVER_ZONE = os.getenv("SILVER_ZONE")


def build_transform_query(bronze_path: str, silver_path: str, airflow_run_id: str) -> str:
    """
    Constructs the DuckDB SQL statement to read the Bronze Parquet file,
    cleanse the data, inject V2 audit columns, and write out to the Silver Parquet file.
    """
    # Windows paths use '\', but DuckDB's SQL parser treats '\' as an escape character.
    # We must convert to universal forward slashes '/' so the SQL string doesn't crash.
    bronze_path = bronze_path.replace("\\", "/")
    silver_path = silver_path.replace("\\", "/")

    # Generate the current UTC timestamp for the _transformed_at audit column
    current_utc = datetime.now(timezone.utc).isoformat()
    
    # If airflow_run_id is None, we must inject the word "NULL" into the SQL string.
    # Otherwise, we wrap the string in single quotes to make it a valid SQL VARCHAR.
    airflow_val = f"'{airflow_run_id}'" if airflow_run_id else "NULL"

    return f"""
    COPY (
        WITH base_data AS (
            SELECT
                -- 1. TYPE CASTING: Force correct data types. Bad data becomes NULL.
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

                -- 2. MISSING DATA HANDLING: Filter out rows where date_added contains corruption symbols (#)
                CASE
                    WHEN date_added LIKE '%#%' THEN NULL
                    ELSE TRY_CAST(date_added AS VARCHAR)
                END AS date_added,

                -- 3. COALESCE: Fill in empty agency/agent with defaults
                COALESCE(NULLIF(TRIM(agency), ''), 'Direct Listing') AS agency,
                COALESCE(NULLIF(TRIM(agent),  ''), 'Not Specified')  AS agent,

                -- 4. UNIT STANDARDIZATION: Standardize Area Type to Marla (1 Kanal = 20 Marla)
                CASE
                    WHEN "Area Type" ILIKE 'Kanal' THEN TRY_CAST("Area Size" AS DOUBLE) * 20.0
                    ELSE TRY_CAST("Area Size" AS DOUBLE)
                END AS area_marla,

                -- Retain Bronze audit columns
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

            -- 5. BUSINESS LOGIC: Price capping for outlier stabilization (capped at 500M PKR)
            CASE
                WHEN raw_price > 500000000 THEN 500000000
                ELSE raw_price
            END AS price,

            -- 6. BUSINESS LOGIC: Calculate price per Marla unit
            CASE
                WHEN area_marla > 0 THEN raw_price / area_marla
                ELSE NULL
            END AS price_per_marla,

            location,
            city,
            province_name,

            -- 7. BUSINESS LOGIC: Validate coordinates using the geographic bounds of Pakistan
            CASE WHEN latitude  BETWEEN 24.0 AND 37.0 THEN latitude  ELSE NULL END AS latitude,
            CASE WHEN longitude BETWEEN 61.0 AND 78.0 THEN longitude ELSE NULL END AS longitude,

            baths,
            bedrooms,
            purpose,
            date_added,
            agency,
            agent,
            area_marla,

            -- 8. AUDIT COLUMNS: Preserve Bronze lineage and add Silver timestamp
            _ingested_at,
            _bronze_airflow_run_id,
            '{current_utc}' AS _transformed_at,
            {airflow_val}   AS _silver_airflow_run_id

        FROM base_data
    ) TO '{silver_path}' (FORMAT PARQUET);
    """


def get_eligible_bronze_files():
    """
    Queries Neon DB to find all files that successfully passed the BRONZE layer.
    Returns a list of tuples: [(file_hash, bronze_file_name), ...]
    """
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT file_hash, file_name FROM file_lineage WHERE layer='BRONZE' AND status='SUCCESS'")
            return cur.fetchall()


def transform_to_silver(batch_limit: int = 0, airflow_run_id: str = None):
    """
    Reads Bronze Parquet files, transforms them via DuckDB, 
    and writes to the Silver zone. Logs lineage via LineageTracker.
    """
    if not BRONZE_ZONE or not SILVER_ZONE:
        logging.error("BRONZE_ZONE or SILVER_ZONE is not set in the .env file. Aborting.")
        return

    os.makedirs(SILVER_ZONE, exist_ok=True)

    # ':memory:' creates a temporary, ultra-fast database entirely in RAM that vanishes when the script ends.
    duckdb_conn = duckdb.connect(database=':memory:')

    # Get the list of eligible files directly from the database (V2 Architecture)
    eligible_files = get_eligible_bronze_files()
    logging.info(f"Found {len(eligible_files)} Parquet file(s) eligible for Silver transformation.")

    if not eligible_files:
        logging.info("Nothing to transform. Exiting.")
        return

    processed_count = 0

    with LineageTracker("SILVER", airflow_run_id=airflow_run_id) as tracker:
        for file_hash, bronze_filename in eligible_files:
            
            if batch_limit > 0 and processed_count >= batch_limit:
                logging.info(f"Batch limit of {batch_limit} reached. Stopping.")
                break

            silver_filename = bronze_filename.replace(".parquet", "_clean.parquet")
            
            bronze_path = os.path.join(BRONZE_ZONE, bronze_filename)
            silver_path = os.path.join(SILVER_ZONE, silver_filename)

            # Step 1: Check the in-memory cache — skip if already SUCCESS in SILVER
            if tracker.is_file_processed(file_hash):
                logging.info(f"SKIP: {silver_filename} (already processed).")
                continue

            # Ensure the source Bronze file actually exists on disk before transforming
            if not os.path.exists(bronze_path):
                logging.warning(f"File missing on disk: {bronze_path}. Skipping.")
                continue

            try:
                # Compile and execute the DuckDB transformation query
                query = build_transform_query(bronze_path, silver_path, airflow_run_id)
                duckdb_conn.execute(query)

                file_size = os.path.getsize(silver_path)
                
                # Syntax Breakdown:
                # 1. execute(...) returns a 2D table object.
                # 2. .fetchone() grabs the first row as a tuple: e.g., (15420,)
                # 3. [0] extracts the integer out of the tuple: 15420
                row_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM read_parquet('{silver_path.replace('\\\\', '/')}')").fetchone()[0]

                # Log SUCCESS to the lineage tracker buffer
                tracker.log_result(
                    file_hash=file_hash,
                    file_name=silver_filename,
                    status="SUCCESS",
                    row_count=row_count,
                    file_size_bytes=file_size
                )
                
                processed_count += 1
                logging.info(f"SUCCESS: {silver_filename} ({row_count:,} rows, {file_size / 1e6:.1f} MB)")

            except Exception as e:
                # String slicing [:500] prevents massive 10k-character stack traces from bloating the Neon PostgreSQL database.
                error_msg = str(e)[:500]
                logging.error(f"FAILED: {bronze_filename} — {error_msg}")
                
                tracker.log_result(
                    file_hash=file_hash,
                    file_name=silver_filename,
                    status="FAILED",
                    error_message=error_msg
                )
                
                # Fail in place! We do NOT move it to quarantine.
                # But we DO clean up any partial Silver Parquet file.
                if os.path.exists(silver_path):
                    os.remove(silver_path)

    logging.info(f"Silver transformation complete. {processed_count}/{len(eligible_files)} file(s) transformed successfully.")


if __name__ == "__main__":
    transform_to_silver()
