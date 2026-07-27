import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.helper_files.database import DBConnection
from src.helper_files.cloud_utils import get_fs_and_options

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
SILVER_ZONE = os.getenv("SILVER_ZONE")

def reset_silver_layer():
    logging.info("🚀 Starting Silver Layer Teardown...")

    # 1. Reset Postgres Lineage for Silver
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM file_lineage WHERE layer = 'SILVER'")
            logging.info("✅ Deleted SILVER lineage records from PostgreSQL.")

    # 2. Delete Physical Files in Silver Zone
    if SILVER_ZONE:
        silver_fs, _ = get_fs_and_options(SILVER_ZONE)
        if silver_fs.exists(SILVER_ZONE):
            files = silver_fs.ls(SILVER_ZONE)
            for f in files:
                silver_fs.rm(f, recursive=True)
            logging.info(f"✅ Deleted all physical files in {SILVER_ZONE}")

    logging.info("🎉 Silver Reset Complete! Pipeline is rolled back to the end of Bronze.")

if __name__ == "__main__":
    reset_silver_layer()
