"""
PropIntel Silver Layer ETL.

Reads Bronze-layer Parquet files and applies data quality transformations
using DuckDB's in-memory SQL engine. Outputs cleaned Parquet files to the
Silver zone. Tracks lineage via Neon DB.

Usage:
    python move_to_silver.py            # Process all eligible files
    python move_to_silver.py --batch 3  # Process only 3 files
"""

import os
import argparse
import duckdb
import psycopg2
from dotenv import load_dotenv

# Load variables from the .env file
load_dotenv()

# --- Configuration ---
NEON_CONN_STR = os.getenv("DATABASE_URL")
if not NEON_CONN_STR:
    raise ValueError("CRITICAL ERROR: Python cannot find DATABASE_URL. Check your .env file!")

# Derive all paths relative to this script's location.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

BRONZE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "bronze")
SILVER_ZONE = os.path.join(WORKSPACE_ROOT, "data", "silver")

# Ensure Silver directory exists
os.makedirs(SILVER_ZONE, exist_ok=True)


def get_neon_connection():
    """Opens a fresh connection to Neon DB."""
    return psycopg2.connect(NEON_CONN_STR)


def ensure_neon_alive(conn, cur):
    """
    Checks if the Neon connection is still alive. If it dropped (serverless cold start),
    opens a fresh connection and returns the new conn/cur pair.
    """
    try:
        cur.execute("SELECT 1;")
        return conn, cur
    except psycopg2.OperationalError:
        print("  Neon connection dropped. Reconnecting...")
        conn = get_neon_connection()
        cur = conn.cursor()
        return conn, cur


def main():
    # --- CLI Arguments ---
    parser = argparse.ArgumentParser(description="PropIntel Silver Layer ETL")
    parser.add_argument("--batch", type=int, default=0,
                        help="Number of files to process. 0 = all eligible files (default).")
    args = parser.parse_args()

    print("Connecting to Pipeline Brain (Neon)...")
    conn = get_neon_connection()
    cur = conn.cursor()

    # 1. Start the Run
    cur.execute("INSERT INTO pipeline_runs (status) VALUES ('RUNNING') RETURNING run_id;")
    run_id = cur.fetchone()[0]
    conn.commit()
    print(f"Started Silver Run ID: {run_id}")

    # 2. Find eligible files (Bronze SUCCESS, no Silver SUCCESS yet)
    cur.execute("""
        SELECT b.file_hash, b.file_name 
        FROM file_lineage b
        LEFT JOIN file_lineage s 
          ON b.file_hash = s.file_hash AND s.layer = 'SILVER' AND s.status = 'SUCCESS'
        WHERE b.layer = 'BRONZE' AND b.status = 'SUCCESS'
          AND s.file_hash IS NULL;
    """)

    pending_files = cur.fetchall()
    total_pending = len(pending_files)

    print(f"Found {total_pending} Bronze files waiting for Silver transformation.")

    if total_pending == 0:
        print("Nothing to process. Exiting.")
        cur.execute("UPDATE pipeline_runs SET status = 'SUCCESS', ended_at = CURRENT_TIMESTAMP WHERE run_id = %s;", (run_id,))
        conn.commit()
        cur.close()
        conn.close()
        return

    # 3. Determine batch size from CLI argument
    batch_size = args.batch if args.batch > 0 else total_pending
    batch_size = min(batch_size, total_pending)
    target_batch = pending_files[:batch_size]

    # Initialize DuckDB (in-memory)
    db = duckdb.connect(database=':memory:')

    # 4. DuckDB Execution
    print(f"Processing {batch_size} files through DuckDB Engine...")
    success_count = 0
    fail_count = 0

    for file_hash, file_name in target_batch:
        bronze_filename = file_name.replace(".csv", ".parquet")
        silver_filename = file_name.replace(".csv", "_clean.parquet")

        bronze_path = os.path.join(BRONZE_ZONE, bronze_filename).replace("\\", "/")
        silver_path = os.path.join(SILVER_ZONE, silver_filename).replace("\\", "/")

        # Pre-log to DB
        conn, cur = ensure_neon_alive(conn, cur)
        cur.execute("""
            INSERT INTO file_lineage (file_hash, layer, file_name, run_id, status)
            VALUES (%s, 'SILVER', %s, %s, 'PROCESSING')
            ON CONFLICT (file_hash, layer) 
            DO UPDATE SET status = 'PROCESSING', run_id = EXCLUDED.run_id, updated_at = CURRENT_TIMESTAMP;
        """, (file_hash, silver_filename, run_id))
        conn.commit()

        try:
            # --- SILVER TRANSFORMATION (DuckDB SQL) ---
            clean_query = f"""
            COPY (
                WITH base_data AS (
                    SELECT 
                        TRY_CAST(property_id AS BIGINT) AS property_id,
                        TRY_CAST(location_id AS INTEGER) AS location_id,
                        page_url,
                        MD5(page_url) AS url_hash,
                        property_type,
                        TRY_CAST(price AS BIGINT) AS raw_price,
                        location,
                        city,
                        province_name,
                        TRY_CAST(latitude AS DOUBLE) AS latitude,
                        TRY_CAST(longitude AS DOUBLE) AS longitude,
                        TRY_CAST(baths AS INTEGER) AS baths,
                        TRY_CAST(bedrooms AS INTEGER) AS bedrooms,
                        purpose,
                        
                        CASE 
                            WHEN date_added LIKE '%#%' THEN NULL 
                            ELSE TRY_CAST(date_added AS VARCHAR) 
                        END AS date_added,
                        
                        COALESCE(NULLIF(TRIM(agency), ''), 'Direct Listing') AS agency,
                        COALESCE(NULLIF(TRIM(agent), ''), 'Not Specified') AS agent,
                        
                        CASE 
                            WHEN "Area Type" ILIKE 'Kanal' THEN TRY_CAST("Area Size" AS DOUBLE) * 20.0
                            ELSE TRY_CAST("Area Size" AS DOUBLE)
                        END AS area_marla,
                        
                        TRY_CAST(snapshot_date AS DATE) AS effective_start_date
                        
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
                    
                    CASE 
                        WHEN latitude BETWEEN 24.0 AND 37.0 THEN latitude 
                        ELSE NULL 
                    END AS latitude,
                    CASE 
                        WHEN longitude BETWEEN 61.0 AND 78.0 THEN longitude 
                        ELSE NULL 
                    END AS longitude,
                    
                    baths,
                    bedrooms,
                    purpose,
                    date_added,
                    agency,
                    agent,
                    area_marla,
                    
                    effective_start_date,
                    CAST('9999-12-31' AS DATE) AS effective_end_date,
                    TRUE AS is_active
                    
                FROM base_data
            ) TO '{silver_path}' (FORMAT PARQUET);
            """

            db.execute(clean_query)

            # Audit Row Count
            count_query = f"SELECT COUNT(*) FROM read_parquet('{silver_path}')"
            row_count = db.execute(count_query).fetchone()[0]

            # Post-log Success
            conn, cur = ensure_neon_alive(conn, cur)
            cur.execute("""
                UPDATE file_lineage 
                SET status = 'SUCCESS', row_count = %s, updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s AND layer = 'SILVER';
            """, (row_count, file_hash))
            conn.commit()

            print(f"  [OK] {silver_filename} ({row_count:,} rows)")
            success_count += 1

        except Exception as e:
            error_msg = str(e)
            print(f"  [FAIL] {file_name}: {error_msg}")

            conn, cur = ensure_neon_alive(conn, cur)
            cur.execute("""
                UPDATE file_lineage 
                SET status = 'FAILED', error_message = %s, updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s AND layer = 'SILVER';
            """, (error_msg, file_hash))
            conn.commit()
            fail_count += 1

    # 5. Finish Run
    final_status = 'SUCCESS' if fail_count == 0 else 'PARTIAL_SUCCESS'
    conn, cur = ensure_neon_alive(conn, cur)
    cur.execute("UPDATE pipeline_runs SET status = %s, ended_at = CURRENT_TIMESTAMP WHERE run_id = %s;",
                (final_status, run_id))
    conn.commit()

    print(f"\nSilver batch complete: {success_count} succeeded, {fail_count} failed.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()