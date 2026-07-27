import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.helper_files.database import DBConnection
from src.helper_files.cloud_utils import get_fs_and_options
from src.helper_files.iceberg_catalog import get_catalog

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
GOLD_ZONE = os.getenv("GOLD_ZONE")

def reset_gold_layer():
    logging.info("🚀 Starting Gold Layer Teardown...")

    # 1. Reset Postgres Lineage for Gold
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM file_lineage WHERE layer = 'GOLD'")
            logging.info("✅ Deleted GOLD lineage records from PostgreSQL.")
            
    # 2. Drop Iceberg Tables
    catalog = get_catalog()
    try:
        catalog.drop_table("propintel.gold_sales")
        logging.info("✅ Dropped Iceberg table: propintel.gold_sales")
    except Exception:
        logging.info("⚠️ propintel.gold_sales table not found or already dropped.")

    try:
        catalog.drop_table("propintel.gold_rentals")
        logging.info("✅ Dropped Iceberg table: propintel.gold_rentals")
    except Exception:
        logging.info("⚠️ propintel.gold_rentals table not found or already dropped.")

    # 3. Delete Physical Files in Gold Zone
    if GOLD_ZONE:
        gold_fs, _ = get_fs_and_options(GOLD_ZONE)
        if gold_fs.exists(GOLD_ZONE):
            files = gold_fs.ls(GOLD_ZONE)
            for f in files:
                gold_fs.rm(f, recursive=True)
            logging.info(f"✅ Deleted all physical warehouse files in {GOLD_ZONE}")
        
    logging.info("🎉 Gold Reset Complete! Pipeline is rolled back to the end of Silver.")

if __name__ == "__main__":
    reset_gold_layer()
