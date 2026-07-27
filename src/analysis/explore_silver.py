import duckdb
import os
import sys
from dotenv import load_dotenv

# Fix path to load helper_files
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.helper_files.cloud_utils import get_fs_and_options

load_dotenv()
SILVER_ZONE = os.getenv("SILVER_ZONE")

def explore_silver_cloud():
    """
    Connects to Azure Blob Storage, reads the cleaned Silver Parquet files,
    and runs validation queries to ensure transformation was successful.
    """
    print(f"📡 Connecting to Azure Silver Zone: {SILVER_ZONE}")
    silver_fs, _ = get_fs_and_options(SILVER_ZONE)

    # 1. List all Silver files
    try:
        silver_files = [f for f in silver_fs.ls(SILVER_ZONE) if f.endswith('_clean.parquet')]
        if not silver_files:
            print("❌ No '_clean.parquet' files found in the Silver container.")
            return
    except Exception as e:
        print(f"❌ Failed to access Silver container: {e}")
        return

    # 2. Lightning Fast Local Staging (To bypass slow network streams)
    import concurrent.futures
    import shutil
    
    local_tmp_dir = "C:/tmp/propintel_silver_exploration"
    if os.path.exists(local_tmp_dir):
        shutil.rmtree(local_tmp_dir)
    os.makedirs(local_tmp_dir, exist_ok=True)
    
    print(f"🚀 High-speed downloading {len(silver_files)} files to {local_tmp_dir}...")
    
    def download_file(blob_path):
        local_path = os.path.join(local_tmp_dir, os.path.basename(blob_path))
        silver_fs.get_file(blob_path, local_path)
        return local_path
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        local_files = list(executor.map(download_file, silver_files))
        
    print("✅ Download complete! Running queries locally at lightning speed...")

    # 3. Setup DuckDB
    conn = duckdb.connect(':memory:')
    query_path = f"{local_tmp_dir}/*.parquet"

    print(f"\n--- 🕵️ Data Quality Checks across {len(silver_files)} files ---")

    # A. Check Total Rows
    row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{query_path}')").fetchone()[0]
    print(f"Total Rows: {row_count:,}")

    # B. Check Column Schema (Did the new columns get added?)
    columns = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{query_path}')").fetchdf()
    print("\nSchema Verified. Key columns present:")
    expected_cols = ['url_hash', 'price', 'price_per_marla', 'area_marla', '_transformed_at', '_silver_airflow_run_id']
    found_cols = columns['column_name'].tolist()
    for col in expected_cols:
        status = "✅" if col in found_cols else "❌"
        print(f"  {status} {col}")

    # C. Check Missing Values in critical fields
    missing_data_query = f"""
    SELECT 
        COUNT(*) FILTER (WHERE property_id IS NULL) as null_property_id,
        COUNT(*) FILTER (WHERE url_hash IS NULL) as null_url_hash,
        COUNT(*) FILTER (WHERE price IS NULL) as null_price,
        COUNT(*) FILTER (WHERE area_marla IS NULL) as null_area
    FROM read_parquet('{query_path}')
    """
    missing_df = conn.execute(missing_data_query).fetchdf()
    print("\nMissing Value Check (Should be 0 for primary columns):")
    for col in missing_df.columns:
        val = missing_df[col][0]
        status = "✅" if val == 0 else "⚠️"
        print(f"  {status} {col}: {val}")

    # D. Quick Stats on Price
    print("\n--- 💰 Quick Aggregation (Price by City) ---")
    agg_query = f"""
    SELECT 
        city,
        COUNT(*) as total_properties,
        CAST(AVG(price) AS BIGINT) as avg_price,
        CAST(MAX(price) AS BIGINT) as max_price
    FROM read_parquet('{query_path}')
    WHERE city IS NOT NULL
    GROUP BY city
    ORDER BY total_properties DESC
    LIMIT 5
    """
    agg_df = conn.execute(agg_query).fetchdf()
    
    # Format prices nicely
    agg_df['avg_price'] = agg_df['avg_price'].apply(lambda x: f"Rs. {x:,}")
    agg_df['max_price'] = agg_df['max_price'].apply(lambda x: f"Rs. {x:,}")
    
    print(agg_df.to_string(index=False))

if __name__ == "__main__":
    explore_silver_cloud()
