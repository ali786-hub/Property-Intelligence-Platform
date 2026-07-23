import os
import sys
import glob
from dotenv import load_dotenv

# Dynamically add the root project directory to Python's module path so it can find 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.helper_files.database import DBConnection

# Load environment variables
load_dotenv()

BRONZE_ZONE = os.getenv("BRONZE_ZONE")
SILVER_ZONE = os.getenv("SILVER_ZONE")

def hard_reset_pipeline():
    """
    WARNING: DEVELOPMENT UTILITY ONLY.
    This script wipes the file_lineage tracking table and deletes all Parquet files
    in the Bronze and Silver zones so you can test the pipeline from scratch.
    """
    print("🚨 STARTING DEVELOPMENT HARD RESET 🚨")

    # 1. Wipe the PostgreSQL Lineage Table
    try:
        with DBConnection() as conn:
            with conn.cursor() as cur:
                # TRUNCATE deletes all rows in the table instantly
                cur.execute("TRUNCATE TABLE file_lineage;")
                print("✅ [Database] Truncated 'file_lineage' table.")
    except Exception as e:
        print(f"❌ [Database] Failed to truncate table: {e}")

    # 2. Delete all Bronze Parquet Files
    try:
        if BRONZE_ZONE and os.path.exists(BRONZE_ZONE):
            bronze_files = glob.glob(os.path.join(BRONZE_ZONE, "*.parquet"))
            for f in bronze_files:
                os.remove(f)
            print(f"✅ [Bronze Zone] Deleted {len(bronze_files)} file(s).")
    except Exception as e:
        print(f"❌ [Bronze Zone] Failed to clear files: {e}")

    # 3. Delete all Silver Parquet Files
    try:
        if SILVER_ZONE and os.path.exists(SILVER_ZONE):
            silver_files = glob.glob(os.path.join(SILVER_ZONE, "*.parquet"))
            for f in silver_files:
                os.remove(f)
            print(f"✅ [Silver Zone] Deleted {len(silver_files)} file(s).")
    except Exception as e:
        print(f"❌ [Silver Zone] Failed to clear files: {e}")

    print("\n🎉 RESET COMPLETE! The pipeline is ready for a fresh test.")
    print("👉 Next Step: Copy a raw CSV file into your 'landing_zone' folder and run bronze_ingest.py!")

if __name__ == "__main__":
    
    confirm = input("Type 'YES' to wipe the pipeline tracking and Bronze/Silver data: ")
    if confirm == 'YES':
        hard_reset_pipeline()
    else:
        print("Reset aborted.")
