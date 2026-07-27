import os
import sys
import re
import logging
from datetime import datetime, timezone
import duckdb
import pyarrow as pa
from dotenv import load_dotenv

# Dynamically add root project directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.helper_files.iceberg_catalog import get_catalog, ensure_tables_exist
from src.helper_files.lineage import LineageTracker
from src.helper_files.database import DBConnection
from src.helper_files.cloud_utils import get_fs_and_options, inject_duckdb_azure_secret

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silence Azure SDK HTTP Request Spam
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

load_dotenv()
SILVER_ZONE = os.getenv("SILVER_ZONE")


def get_eligible_silver_files():
    """
    Queries DB to find files successfully transformed in SILVER layer.
    Returns: list of (file_hash, silver_file_name)
    """
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT file_hash, file_name FROM file_lineage WHERE layer='SILVER' AND status='SUCCESS'")
            return cur.fetchall()


def extract_date_from_filename(filename):
    """
    Extracts the date string from a Silver filename.
    Example: 'property_data_2025-03-01_clean.parquet' -> '2025-03-01'
    Returns the date string, or None if no date pattern is found.
    """
    match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    return match.group(1) if match else None


# =============================================================
# MODE 1: DAILY INCREMENTAL (1 day's files, merge with existing Gold)
# =============================================================
def process_table_incremental(catalog, file_list, file_date, purpose_filter, iceberg_table_name, airflow_run_id, silver_fs):
    """
    Processes a single day's Silver files against existing Gold data.
    Uses the SCD Type 2 UNION ALL merge strategy.
    
    This is the standard production path used when Airflow triggers
    the pipeline daily and only 1 new day of data exists.
    
    Args:
        file_date: The date string extracted from the filename (e.g. '2025-03-01').
                   Used for valid_from/valid_to instead of datetime.now().
    """
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='2GB'")
    
    if SILVER_ZONE and (SILVER_ZONE.startswith("abfs://") or SILVER_ZONE.startswith("azure://")):
        con.register_filesystem(silver_fs)
    
    files_sql = "[" + ", ".join(f"'{f}'" for f in file_list) + "]"
    
    table = catalog.load_table(iceberg_table_name)
    current_utc = datetime.now(timezone.utc).isoformat()
    
    # Check if table has data by scanning it
    has_data = len(table.scan().to_arrow()) > 0
    
    if has_data:
        # Load existing Iceberg data into DuckDB as an Arrow table
        existing_arrow = table.scan().to_arrow()
        con.register('existing_gold', existing_arrow)
    else:
        # Initialize an empty typed view for initial loads.
        # This prevents schema validation errors during the SCD2 LEFT JOIN 
        # when the target Iceberg table contains no historical data.
        con.execute("""
            CREATE OR REPLACE TEMP VIEW existing_gold AS 
            SELECT 
                CAST(NULL AS BIGINT) as property_id,
                CAST(NULL AS INTEGER) as location_id,
                CAST(NULL AS VARCHAR) as page_url,
                CAST(NULL AS VARCHAR) as url_hash,
                CAST(NULL AS VARCHAR) as property_type,
                CAST(NULL AS VARCHAR) as property_category,
                CAST(NULL AS BIGINT) as price,
                CAST(NULL AS DOUBLE) as price_per_marla,
                CAST(NULL AS BOOLEAN) as is_anomaly,
                CAST(NULL AS VARCHAR) as location,
                CAST(NULL AS VARCHAR) as city,
                CAST(NULL AS VARCHAR) as province_name,
                CAST(NULL AS DOUBLE) as latitude,
                CAST(NULL AS DOUBLE) as longitude,
                CAST(NULL AS INTEGER) as baths,
                CAST(NULL AS INTEGER) as bedrooms,
                CAST(NULL AS VARCHAR) as agency,
                CAST(NULL AS VARCHAR) as agent,
                CAST(NULL AS DOUBLE) as area_marla,
                CAST(NULL AS DATE) as date_added,
                CAST(NULL AS BOOLEAN) as is_current,
                CAST(NULL AS DATE) as valid_from,
                CAST(NULL AS DATE) as valid_to,
                CAST(NULL AS VARCHAR) as _ingested_at,
                CAST(NULL AS VARCHAR) as _bronze_airflow_run_id,
                CAST(NULL AS VARCHAR) as _transformed_at,
                CAST(NULL AS VARCHAR) as _silver_airflow_run_id,
                CAST(NULL AS VARCHAR) as _gold_loaded_at
            WHERE 1=0
        """)

    # Define baseline validity thresholds to flag potential upstream data anomalies
    anomaly_threshold = 100000 if purpose_filter == 'For Sale' else 1000
    
    # Register incoming silver batch into DuckDB memory with gold schema transformations
    # FIX: Uses file_date (from filename) instead of datetime.now() for accurate SCD2 timelines
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW incoming_silver AS
        SELECT 
            *,
            CASE WHEN bedrooms = 0 THEN 'Plot/Land' ELSE property_type END AS property_category,
            CASE WHEN price < {anomaly_threshold} THEN TRUE ELSE FALSE END AS is_anomaly,
            '{file_date}'::DATE AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            TRUE AS is_current,
            '{current_utc}' AS _gold_loaded_at
        FROM read_parquet({files_sql})
        WHERE purpose = '{purpose_filter}'
    """)
    
    # ---------------------------------------------------------
    # SCD TYPE 2 LOGIC (INCREMENTAL MERGE)
    # ---------------------------------------------------------
    # 1. Unchanged and Historic Records: Keep exactly as they are in existing_gold
    # 2. Expired Records: If a current record has a new price in incoming_silver, expire it.
    # 3. New Records: Any record in incoming_silver that doesn't match an existing current record's ID & price.
    
    scd2_query = f"""
        -- SCD2 Merge Strategy: Union disjoint sets (Unchanged, Expired, New) 
        -- into a single snapshot to overwrite the target Iceberg table.
        
        -- Group 1: All existing records that are NOT being updated today
        SELECT g.*
        FROM existing_gold g
        LEFT JOIN incoming_silver s 
            ON g.property_id = s.property_id 
            AND g.is_current = TRUE 
            AND g.price != s.price
        WHERE s.property_id IS NULL
        
        UNION ALL           
        
        -- Group 2: The OLD versions of records that ARE being updated today (mark as expired)
        SELECT 
            g.property_id, g.location_id, g.page_url, g.url_hash, g.property_type, g.property_category,
            g.price, g.price_per_marla, g.is_anomaly, g.location, g.city, g.province_name, g.latitude, g.longitude,
            g.baths, g.bedrooms, g.agency, g.agent, g.area_marla, g.date_added,
            FALSE AS is_current,
            g.valid_from,
            '{file_date}'::DATE AS valid_to,
            g._ingested_at, g._bronze_airflow_run_id, g._transformed_at, g._silver_airflow_run_id,
            '{current_utc}' AS _gold_loaded_at
        FROM existing_gold g
        INNER JOIN incoming_silver s 
            ON g.property_id = s.property_id 
            AND g.is_current = TRUE 
            AND g.price != s.price
            
        UNION ALL
        
        -- Group 3: The NEW incoming records
        SELECT 
            s.property_id, s.location_id, s.page_url, s.url_hash, s.property_type, s.property_category,
            s.price, s.price_per_marla, s.is_anomaly, s.location, s.city, s.province_name, s.latitude, s.longitude,
            s.baths, s.bedrooms, s.agency, s.agent, s.area_marla, s.date_added,
            s.is_current,
            s.valid_from,
            s.valid_to,
            s._ingested_at, s._bronze_airflow_run_id, s._transformed_at, s._silver_airflow_run_id,
            s._gold_loaded_at
        FROM incoming_silver s
        LEFT JOIN existing_gold g 
            ON s.property_id = g.property_id 
            AND g.is_current = TRUE 
            AND s.price = g.price
        WHERE g.property_id IS NULL
    """
    
    # Execute the SCD2 merge and output to PyArrow
    final_arrow_table = con.execute(scd2_query).fetch_arrow_table()
    
    # Overwrite the Iceberg table with the new snapshot
    if len(final_arrow_table) > 0:
        table.overwrite(final_arrow_table)
        logger.info(f"Successfully overwrote {iceberg_table_name} with {len(final_arrow_table)} total rows.")
    else:
        logger.info(f"No data to write for {iceberg_table_name}.")
        
    con.close()
    return len(final_arrow_table)


# =============================================================
# MODE 2: BULK BACKFILL (multiple days, empty Gold, single pass)
# =============================================================
def process_table_bulk(catalog, file_list, purpose_filter, iceberg_table_name, airflow_run_id, silver_fs):
    """
    Bulk-loads multiple days of Silver files into Gold in a SINGLE pass.
    
    Instead of looping file-by-file (which causes N separate Iceberg read/write cycles),
    this function loads ALL files into DuckDB simultaneously and uses SQL Window Functions
    (LAG/LEAD) to calculate the entire SCD2 timeline in one query.
    
    Result: 1 Iceberg write instead of N. Processes 120 files in ~15 seconds.
    
    This path is used when Gold is empty and multiple days need to be backfilled
    (e.g., initial deployment, disaster recovery, schema migration).
    """
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='8GB'")
    con.execute("PRAGMA temp_directory='/tmp/duckdb_temp'")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA preserve_insertion_order=false")

    if SILVER_ZONE and (SILVER_ZONE.startswith("abfs://") or SILVER_ZONE.startswith("azure://")):
        con.register_filesystem(silver_fs)
    
    files_sql = "[" + ", ".join(f"'{f}'" for f in file_list) + "]"
    table = catalog.load_table(iceberg_table_name)
    current_utc = datetime.now(timezone.utc).isoformat()
    
    anomaly_threshold = 100000 if purpose_filter == 'For Sale' else 1000
    
    # ---------------------------------------------------------
    # PARTITIONED BULK SCD TYPE 2 LOGIC (WINDOW FUNCTIONS)
    # ---------------------------------------------------------
    
    # 1. Fetch Distinct Cities to partition the memory load
    distinct_cities_query = f"SELECT DISTINCT city FROM read_parquet({files_sql}) WHERE purpose = '{purpose_filter}' AND city IS NOT NULL"
    distinct_cities = con.execute(distinct_cities_query).fetchall()
    cities = [row[0] for row in distinct_cities if row[0]]
    
    total_rows = 0
    first_chunk = True
    
    for city in cities:
        escaped_city = city.replace("'", "''")
        logger.info(f"[BULK] Processing SCD2 timeline for city: {city}...")
        
        # Inject the city partition into the CTE
        bulk_scd2_query = f"""
            WITH all_silver AS (
                SELECT 
                    *,
                    regexp_extract(filename, '(\d{{4}}-\d{{2}}-\d{{2}})')::DATE AS scrape_date,
                    CASE WHEN bedrooms = 0 THEN 'Plot/Land' ELSE property_type END AS property_category,
                    CASE WHEN price < {anomaly_threshold} THEN TRUE ELSE FALSE END AS is_anomaly,
                    '{current_utc}' AS _gold_loaded_at
                FROM read_parquet({files_sql}, filename=true)
                WHERE purpose = '{purpose_filter}' AND city = '{escaped_city}'
            ),
            deduped AS (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY property_id, scrape_date ORDER BY scrape_date) as rn
                FROM all_silver
            ),
            unique_rows AS (
                SELECT * EXCLUDE (rn, filename) FROM deduped WHERE rn = 1
            ),
            with_prev_price AS (
                SELECT *,
                       LAG(price) OVER (PARTITION BY property_id ORDER BY scrape_date) as prev_price
                FROM unique_rows
            ),
            price_changes AS (
                SELECT * EXCLUDE (prev_price) 
                FROM with_prev_price
                WHERE prev_price IS NULL OR price != prev_price
            ),
            scd2_timeline AS (
                SELECT 
                    CAST(property_id AS BIGINT) AS property_id,
                    CAST(location_id AS INTEGER) AS location_id,
                    CAST(page_url AS VARCHAR) AS page_url,
                    CAST(url_hash AS VARCHAR) AS url_hash,
                    CAST(property_type AS VARCHAR) AS property_type,
                    CAST(property_category AS VARCHAR) AS property_category,
                    CAST(price AS BIGINT) AS price,
                    CAST(price_per_marla AS DOUBLE) AS price_per_marla,
                    CAST(is_anomaly AS BOOLEAN) AS is_anomaly,
                    CAST(location AS VARCHAR) AS location,
                    CAST(city AS VARCHAR) AS city,
                    CAST(province_name AS VARCHAR) AS province_name,
                    CAST(latitude AS DOUBLE) AS latitude,
                    CAST(longitude AS DOUBLE) AS longitude,
                    CAST(baths AS INTEGER) AS baths,
                    CAST(bedrooms AS INTEGER) AS bedrooms,
                    CAST(agency AS VARCHAR) AS agency,
                    CAST(agent AS VARCHAR) AS agent,
                    CAST(area_marla AS DOUBLE) AS area_marla,
                    CAST(date_added AS DATE) AS date_added,
                    CASE 
                        WHEN LEAD(scrape_date) OVER (PARTITION BY property_id ORDER BY scrape_date) IS NULL 
                        THEN TRUE ELSE FALSE 
                    END AS is_current,
                    CAST(scrape_date AS DATE) AS valid_from,
                    CAST(LEAD(scrape_date) OVER (PARTITION BY property_id ORDER BY scrape_date) AS DATE) AS valid_to,
                    CAST(_ingested_at AS VARCHAR) AS _ingested_at,
                    CAST(_bronze_airflow_run_id AS VARCHAR) AS _bronze_airflow_run_id,
                    CAST(_transformed_at AS VARCHAR) AS _transformed_at,
                    CAST(_silver_airflow_run_id AS VARCHAR) AS _silver_airflow_run_id,
                    CAST(_gold_loaded_at AS VARCHAR) AS _gold_loaded_at
                FROM price_changes
            )
            SELECT * FROM scd2_timeline
        """
        
        city_arrow = con.execute(bulk_scd2_query).fetch_arrow_table()
        
        if len(city_arrow) > 0:
            if first_chunk:
                table.overwrite(city_arrow)
                first_chunk = False
            else:
                table.append(city_arrow)
            
            total_rows += len(city_arrow)
            logger.info(f"[BULK]   -> Wrote {len(city_arrow)} SCD2 rows for {city}")

    if total_rows == 0:
        logger.info(f"[BULK] No data to write for {iceberg_table_name}.")
    else:
        logger.info(f"[BULK] Successfully wrote {iceberg_table_name} with {total_rows} total rows.")
        
    con.close()
    return total_rows


# =============================================================
# ORCHESTRATOR: Intelligent mode selection
# =============================================================
def publish_to_gold(airflow_run_id: str = None):
    if not SILVER_ZONE:
        logger.error("SILVER_ZONE is not set in .env. Aborting.")
        return

    logger.info("Initializing PyIceberg Catalog...")
    catalog = get_catalog()
    ensure_tables_exist(catalog)
    
    silver_fs, _ = get_fs_and_options(SILVER_ZONE)
    
    eligible_silver = get_eligible_silver_files()
    if not eligible_silver:
        logger.info("No eligible Silver files found. Exiting.")
        return

    # Track Lineage
    with LineageTracker('GOLD', airflow_run_id) as tracker:
        files_to_process = []
        
        for file_hash, silver_filename in eligible_silver:
            if not tracker.is_file_processed(file_hash):
                silver_path = f"{SILVER_ZONE}/{silver_filename}"
                if silver_fs.exists(silver_path):
                    files_to_process.append((file_hash, silver_path, silver_filename))
        
        if not files_to_process:
            logger.info("All eligible Silver files have already been processed into Gold. Exiting.")
            return

        # Sort files alphabetically to ensure chronological processing
        sorted_files = sorted(files_to_process, key=lambda x: x[2])
        
        # ---------------------------------------------------------
        # HIGH-SPEED LOCAL STAGING
        # ---------------------------------------------------------
        import concurrent.futures
        import shutil
        local_staging_dir = "/tmp/propintel_gold_staging"
        os.makedirs(local_staging_dir, exist_ok=True)
        
        logger.info(f"🚀 High-speed staging {len(sorted_files)} Silver files to local SSD ({local_staging_dir})...")
        
        def download_to_local(item):
            file_hash, silver_path, silver_filename = item
            local_path = os.path.join(local_staging_dir, silver_filename)
            silver_fs.get_file(silver_path, local_path)
            return file_hash, local_path, silver_filename

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            local_sorted_files = list(executor.map(download_to_local, sorted_files))
            
        logger.info("✅ Local staging complete! DuckDB will query at SSD speed.")
        
        # Extract unique dates from filenames to decide which mode to use
        unique_dates = set()
        for _, _, filename in local_sorted_files:
            date = extract_date_from_filename(filename)
            if date:
                unique_dates.add(date)
        
        # Check if Gold tables already have data
        sales_table = catalog.load_table('propintel.gold_sales')
        gold_has_data = len(sales_table.scan().to_arrow()) > 0
        
        # ---------------------------------------------------------
        # INTELLIGENT MODE SELECTION
        # ---------------------------------------------------------
        # BULK MODE:  Gold is empty AND we have multiple days -> Window function single-pass
        # DAILY MODE: Gold has data OR we have just 1 day     -> Incremental SCD2 merge
        # ---------------------------------------------------------
        
        use_bulk = (not gold_has_data)
        
        if use_bulk:
            # ===================== BULK BACKFILL =====================
            logger.info(f"[BULK MODE] Gold is empty. Processing {len(local_sorted_files)} files across {len(unique_dates)} days in a single pass.")
            
            all_paths = [local_path for _, local_path, _ in local_sorted_files]
            
            try:
                process_table_bulk(catalog, all_paths, 'For Sale', 'propintel.gold_sales', airflow_run_id, silver_fs)
                process_table_bulk(catalog, all_paths, 'For Rent', 'propintel.gold_rentals', airflow_run_id, silver_fs)
                
                # Log ALL files as successfully processed
                for file_hash, _, silver_filename in sorted_files:
                    tracker.log_result(file_hash, silver_filename, 'SUCCESS')
                    
            except Exception as e:
                logger.error(f"[BULK MODE] Error during bulk backfill: {e}")
                for file_hash, _, silver_filename in sorted_files:
                    tracker.log_result(file_hash, silver_filename, 'FAILED', error_message=str(e))
                raise
        else:
            # ===================== DAILY INCREMENTAL =====================
            logger.info(f"[DAILY MODE] Processing {len(local_sorted_files)} file(s) incrementally.")
            
            try:
                for file_hash, local_path, silver_filename in local_sorted_files:
                    file_date = extract_date_from_filename(silver_filename)
                    if not file_date:
                        file_date = datetime.now(timezone.utc).date().isoformat()
                    
                    logger.info(f"--- Processing {silver_filename} (date: {file_date}) ---")
                    
                    process_table_incremental(catalog, [local_path], file_date, 'For Sale', 'propintel.gold_sales', airflow_run_id, silver_fs)
                    process_table_incremental(catalog, [local_path], file_date, 'For Rent', 'propintel.gold_rentals', airflow_run_id, silver_fs)
                    
                    tracker.log_result(file_hash, silver_filename, 'SUCCESS')
                    
            except Exception as e:
                logger.error(f"[DAILY MODE] Error processing Gold Layer: {e}")
                tracker.log_result(file_hash, silver_filename, 'FAILED', error_message=str(e))
                raise
            
    logger.info("Gold Publish Complete.")

if __name__ == "__main__":
    run_id = os.getenv("AIRFLOW_RUN_ID")
    publish_to_gold(airflow_run_id=run_id)
