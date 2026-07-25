import os
import sys
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def process_table(con, catalog, file_list, purpose_filter, iceberg_table_name, airflow_run_id):
    """
    Processes a specific list of Silver Parquet files for a specific purpose (Sale or Rent),
    applies SCD Type 2 logic against the existing Iceberg table,
    and overwrites the Iceberg table with the new state.
    """
    # Create a string representation of the list for DuckDB: ['file1.parquet', 'file2.parquet']
    files_sql = "[" + ", ".join(f"'{f}'" for f in file_list) + "]"
    
    table = catalog.load_table(iceberg_table_name)
    current_utc = datetime.now(timezone.utc).isoformat()
    current_date = datetime.now(timezone.utc).date().isoformat()
    
    # Check if table has data by scanning it
    has_data = len(table.scan().to_arrow()) > 0
    
    if has_data:
        # Load existing Iceberg data into DuckDB as an Arrow table
        existing_arrow = table.scan().to_arrow()
        con.register('existing_gold', existing_arrow)
    else:
        # Create an empty view with the correct schema if Iceberg is empty
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

    # Load Silver Data and enrich it
    anomaly_threshold = 100000 if purpose_filter == 'For Sale' else 1000
    
    # Notice we read from the files_sql list instead of *.parquet
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW incoming_silver AS
        SELECT 
            *,
            CASE WHEN bedrooms = 0 THEN 'Plot/Land' ELSE property_type END AS property_category,
            CASE WHEN price < {anomaly_threshold} THEN TRUE ELSE FALSE END AS is_anomaly,
            '{current_date}'::DATE AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            TRUE AS is_current,
            '{current_utc}' AS _gold_loaded_at
        FROM read_parquet({files_sql})
        WHERE purpose = '{purpose_filter}'
    """)
    
    # ---------------------------------------------------------
    # SCD TYPE 2 LOGIC
    # ---------------------------------------------------------
    # 1. Unchanged and Historic Records: Keep exactly as they are in existing_gold
    #    (Except if they are being updated, in which case we expire them)
    # 2. Expired Records: If a current record has a new price in incoming_silver, expire it.
    # 3. New Records: Any record in incoming_silver that doesn't match an existing current record's ID & price.
    
    scd2_query = f"""
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
            '{current_date}'::DATE AS valid_to,
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
    final_arrow_table = con.execute(scd2_query).arrow()
    
    # Overwrite the Iceberg table with the new snapshot
    if len(final_arrow_table) > 0:
        table.overwrite(final_arrow_table)
        logger.info(f"Successfully overwrote {iceberg_table_name} with {len(final_arrow_table)} total rows.")
    else:
        logger.info(f"No data to write for {iceberg_table_name}.")
        
    return len(final_arrow_table)

def publish_to_gold(airflow_run_id: str = None):
    logger.info("Initializing PyIceberg Catalog...")
    catalog = get_catalog()
    ensure_tables_exist(catalog)
    
    eligible_silver = get_eligible_silver_files()
    if not eligible_silver:
        logger.info("No eligible Silver files found. Exiting.")
        return

    # Track Lineage
    with LineageTracker('GOLD', airflow_run_id) as tracker:
        files_to_process = []
        
        for file_hash, silver_filename in eligible_silver:
            if not tracker.is_file_processed(file_hash):
                silver_path = os.path.join(SILVER_ZONE, silver_filename).replace("\\", "/")
                if os.path.exists(silver_path):
import os
import sys
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def process_table(con, catalog, file_list, purpose_filter, iceberg_table_name, airflow_run_id):
    """
    Processes a specific list of Silver Parquet files for a specific purpose (Sale or Rent),
    applies SCD Type 2 logic against the existing Iceberg table,
    and overwrites the Iceberg table with the new state.
    """
    # Create a string representation of the list for DuckDB: ['file1.parquet', 'file2.parquet']
    files_sql = "[" + ", ".join(f"'{f}'" for f in file_list) + "]"
    
    table = catalog.load_table(iceberg_table_name)
    current_utc = datetime.now(timezone.utc).isoformat()
    current_date = datetime.now(timezone.utc).date().isoformat()
    
    # Check if table has data by scanning it
    has_data = len(table.scan().to_arrow()) > 0
    
    if has_data:
        # Load existing Iceberg data into DuckDB as an Arrow table
        existing_arrow = table.scan().to_arrow()
        con.register('existing_gold', existing_arrow)
    else:
        # Create an empty view with the correct schema if Iceberg is empty
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

    # Load Silver Data and enrich it
    anomaly_threshold = 100000 if purpose_filter == 'For Sale' else 1000
    
    # Notice we read from the files_sql list instead of *.parquet
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW incoming_silver AS
        SELECT 
            *,
            CASE WHEN bedrooms = 0 THEN 'Plot/Land' ELSE property_type END AS property_category,
            CASE WHEN price < {anomaly_threshold} THEN TRUE ELSE FALSE END AS is_anomaly,
            '{current_date}'::DATE AS valid_from,
            CAST(NULL AS DATE) AS valid_to,
            TRUE AS is_current,
            '{current_utc}' AS _gold_loaded_at
        FROM read_parquet({files_sql})
        WHERE purpose = '{purpose_filter}'
    """)
    
    # ---------------------------------------------------------
    # SCD TYPE 2 LOGIC
    # ---------------------------------------------------------
    # 1. Unchanged and Historic Records: Keep exactly as they are in existing_gold
    #    (Except if they are being updated, in which case we expire them)
    # 2. Expired Records: If a current record has a new price in incoming_silver, expire it.
    # 3. New Records: Any record in incoming_silver that doesn't match an existing current record's ID & price.
    
    scd2_query = f"""
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
            '{current_date}'::DATE AS valid_to,
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
    final_arrow_table = con.execute(scd2_query).arrow()
    
    # Overwrite the Iceberg table with the new snapshot
    if len(final_arrow_table) > 0:
        table.overwrite(final_arrow_table)
        logger.info(f"Successfully overwrote {iceberg_table_name} with {len(final_arrow_table)} total rows.")
    else:
        logger.info(f"No data to write for {iceberg_table_name}.")
        
    return len(final_arrow_table)

def publish_to_gold(airflow_run_id: str = None):
    logger.info("Initializing PyIceberg Catalog...")
    catalog = get_catalog()
    ensure_tables_exist(catalog)
    
    eligible_silver = get_eligible_silver_files()
    if not eligible_silver:
        logger.info("No eligible Silver files found. Exiting.")
        return

    # Track Lineage
    with LineageTracker('GOLD', airflow_run_id) as tracker:
        files_to_process = []
        
        for file_hash, silver_filename in eligible_silver:
            if not tracker.is_file_processed(file_hash):
                silver_path = os.path.join(SILVER_ZONE, silver_filename).replace("\\", "/")
                if os.path.exists(silver_path):
                    files_to_process.append((file_hash, silver_path, silver_filename))
        
        if not files_to_process:
            logger.info("All eligible Silver files have already been processed into Gold. Exiting.")
            return

        # Sort files alphabetically to ensure chronological processing
        # e.g., property_data_2025-03-01 comes before 2025-03-02
        sorted_files = sorted(files_to_process, key=lambda x: x[2])
        
        logger.info(f"Found {len(sorted_files)} new Silver file(s) to process sequentially.")
        
        con = duckdb.connect()
        con.execute("PRAGMA memory_limit='2GB'")
        
        try:
            for file_hash, silver_path, silver_filename in sorted_files:
                logger.info(f"--- Processing {silver_filename} ---")
                
                # Process Sales
                process_table(con, catalog, [silver_path], 'For Sale', 'propintel.gold_sales', airflow_run_id)
                
                # Process Rentals
                process_table(con, catalog, [silver_path], 'For Rent', 'propintel.gold_rentals', airflow_run_id)
                
                # Log success for this specific file
                tracker.log_result(file_hash, silver_filename, 'SUCCESS')
                
        except Exception as e:
            logger.error(f"Error processing Gold Layer: {e}")
            # If a file fails, log the failure. Note: we don't know exactly which one failed 
            # if the exception happened outside the loop, but here it happens inside.
            # We should only log failure for the one that crashed.
            tracker.log_result(file_hash, silver_filename, 'FAILED', error_message=str(e))
            raise
        finally:
            con.close()
            
    logger.info("Gold Publish Complete.")

if __name__ == "__main__":
    publish_to_gold(airflow_run_id="manual_test")
