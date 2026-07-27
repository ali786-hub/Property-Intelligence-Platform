import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.helper_files.database import DBConnection
from src.helper_files.cloud_utils import get_fs_and_options

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
BRONZE_ZONE = os.getenv("BRONZE_ZONE")

def reset_bronze_layer():
    logging.info("🚀 Starting Bronze Layer Teardown...")

    # 1. Reset Postgres Lineage for Bronze
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM file_lineage WHERE layer = 'BRONZE'")
            logging.info("✅ Deleted BRONZE lineage records from PostgreSQL.")

    # 2. Delete Physical Files in Bronze Zone
    if BRONZE_ZONE:
        bronze_fs, _ = get_fs_and_options(BRONZE_ZONE)
        if bronze_fs.exists(BRONZE_ZONE):
            files = bronze_fs.ls(BRONZE_ZONE)
            for f in files:
                bronze_fs.rm(f, recursive=True)
            logging.info(f"✅ Deleted all physical files in {BRONZE_ZONE}")

    logging.info("🎉 Bronze Reset Complete! Pipeline is rolled back to the Landing Zone.")

if __name__ == "__main__":
    reset_bronze_layer()
