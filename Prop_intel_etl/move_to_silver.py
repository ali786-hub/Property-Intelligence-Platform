"""
PropIntel Silver Layer ETL.

Reads Bronze Parquet files, cleanses, standardizes and validates the data 
using DuckDB SQL queries, and outputs enriched Silver Parquet tables.
"""

import os
import argparse
import duckdb

from pipeline_tracker import PipelineTracker

# --- Path Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

BRONZE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "bronze")
SILVER_ZONE = os.path.join(WORKSPACE_ROOT, "data", "silver")

os.makedirs(SILVER_ZONE, exist_ok=True)


def build_transform_query(bronze_path: str, silver_path: str) -> str:
    """
    Constructs the DuckDB SQL statement to transform a Bronze Parquet file 
    into a cleansed Silver Parquet table.
    
    Args:
        bronze_path: Path to the input Bronze file.
        silver_path: Path to the output Silver file.
    Returns:
        SQL query string.
    """
    # Normalize paths to use forward slashes for DuckDB cross-platform compatibility
    normalized_bronze_path = bronze_path.replace("\\", "/")
    normalized_silver_path = silver_path.replace("\\", "/")

    return f"""
    COPY (
        WITH base_data AS (
            SELECT
                TRY_CAST(property_id AS BIGINT)     AS property_id,
                TRY_CAST(location_id AS INTEGER)    AS location_id,
                page_url,
                MD5(page_url)                       AS url_hash,
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

                -- Filter out rows where date_added contains corruption symbols (#)
                CASE
                    WHEN date_added LIKE '%#%' THEN NULL
                    ELSE TRY_CAST(date_added AS VARCHAR)
                END AS date_added,

                -- Handle missing values for agency/agent
                COALESCE(NULLIF(TRIM(agency), ''), 'Direct Listing') AS agency,
                COALESCE(NULLIF(TRIM(agent),  ''), 'Not Specified')  AS agent,

                -- Standardize Area Type to Marla (1 Kanal = 20 Marla)
                CASE
                    WHEN "Area Type" ILIKE 'Kanal' THEN TRY_CAST("Area Size" AS DOUBLE) * 20.0
                    ELSE TRY_CAST("Area Size" AS DOUBLE)
                END AS area_marla,

                TRY_CAST(snapshot_date AS DATE) AS effective_start_date

            FROM read_parquet('{normalized_bronze_path}')
        )
        SELECT
            property_id,
            location_id,
            url_hash,
            page_url,
            property_type,

            -- Price capping for outlier stabilization (capped at 500M PKR)
            CASE
                WHEN raw_price > 500000000 THEN 500000000
                ELSE raw_price
            END AS price,

            -- Calculate price per Marla unit
            CASE
                WHEN area_marla > 0 THEN raw_price / area_marla
                ELSE NULL
            END AS price_per_marla,

            location,
            city,
            province_name,

            -- Validate coordinates using the geographic bounds of Pakistan
            CASE WHEN latitude  BETWEEN 24.0 AND 37.0 THEN latitude  ELSE NULL END AS latitude,
            CASE WHEN longitude BETWEEN 61.0 AND 78.0 THEN longitude ELSE NULL END AS longitude,

            baths,
            bedrooms,
            purpose,
            date_added,
            agency,
            agent,
            area_marla,

            -- SCD2 (Slowly Changing Dimension Type 2) tracking metadata
            effective_start_date,
            CAST('9999-12-31' AS DATE) AS effective_end_date,
            TRUE                       AS is_active

        FROM base_data
    ) TO '{normalized_silver_path}' (FORMAT PARQUET);
    """


def main():
    parser = argparse.ArgumentParser(description="PropIntel Silver Layer Cleansing")
    parser.add_argument("--batch", type=int, default=0,
                        help="Maximum number of files to process. 0 = process all (default).")
    args = parser.parse_args()

    with PipelineTracker('SILVER') as tracker:
        eligible_files = tracker.get_eligible_files()
        total_eligible = len(eligible_files)

        print(f"Found {total_eligible} Bronze file(s) eligible for Silver transformation.")

        if total_eligible == 0:
            print("Nothing to process.")
            return

        # Determine batch slice
        batch_limit = args.batch if args.batch > 0 else total_eligible
        batch_size = min(batch_limit, total_eligible)
        target_batch = eligible_files[:batch_size]
        print(f"Processing batch of {batch_size} file(s)...\n")

        # Establish single in-memory DuckDB connection for execution speed
        duckdb_conn = duckdb.connect(database=':memory:')

        for file_hash, file_name in target_batch:
            bronze_filename = file_name
            silver_filename = file_name.replace(".parquet", "_clean.parquet")

            bronze_path = os.path.join(BRONZE_ZONE, bronze_filename)
            silver_path = os.path.join(SILVER_ZONE, silver_filename)

            # Register file state as PROCESSING
            tracker.pre_log(file_hash, silver_filename)

            try:
                # Compile and execute transformation query
                query = build_transform_query(bronze_path, silver_path)
                duckdb_conn.execute(query)

                # Validate output file presence and size
                file_size = os.path.getsize(silver_path)
                if file_size == 0:
                    raise ValueError("Output Silver file is empty.")

                # Calculate rows in the output Parquet table
                escaped_silver_path = silver_path.replace(chr(92), '/')
                row_count = duckdb_conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{escaped_silver_path}')"
                ).fetchone()[0]

                # Update Neon DB lineage state to SUCCESS
                tracker.log_success(file_hash, row_count, file_size)
                print(f"  [OK] {silver_filename} ({row_count:,} rows, {file_size / 1e6:.1f} MB)")

            except Exception as e:
                error_msg = str(e)
                print(f"  [FAIL] {file_name}: {error_msg}")
                tracker.log_failure(file_hash, error_msg)


if __name__ == "__main__":
    main()