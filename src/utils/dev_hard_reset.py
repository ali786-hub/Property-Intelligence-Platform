import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.helper_files.database import DBConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
BRONZE_ZONE = os.getenv("BRONZE_ZONE")
SILVER_ZONE = os.getenv("SILVER_ZONE")
GOLD_ZONE = os.getenv("GOLD_ZONE", "C:/Omnijourney_Kofking_github/data/gold")

def delete_parquet_files(directory):
    if not os.path.exists(directory):
        return
    for file in os.listdir(directory):
        if file.endswith(".parquet"):
            path = os.path.join(directory, file)
            os.remove(path)
            logger.info(f"Deleted: {path}")

def delete_iceberg_catalog(gold_zone):
    import shutil
    from src.helper_files.iceberg_catalog import get_catalog
    
    # 1. Drop the tables from the PostgreSQL catalog
    try:
        catalog = get_catalog()
        try:
            catalog.drop_table("propintel.gold_sales")
            logger.info("Dropped table propintel.gold_sales from catalog.")
        except:
            pass
        try:
            catalog.drop_table("propintel.gold_rentals")
            logger.info("Dropped table propintel.gold_rentals from catalog.")
        except:
            pass
    except Exception as e:
        logger.warning(f"Could not drop Iceberg tables from catalog: {e}")

    # 2. Delete the physical warehouse files
    warehouse_dir = os.path.join(gold_zone, "warehouse")
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)
        logger.info(f"Deleted Iceberg warehouse directory: {warehouse_dir}")

def reset_db_layer(layer_name):
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM file_lineage WHERE layer = %s", (layer_name.upper(),))
            deleted_rows = cur.rowcount
            logger.info(f"Deleted {deleted_rows} lineage records for layer: {layer_name.upper()}")

def nuke_db():
    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE file_lineage")
            logger.info("Nuclear Reset: Truncated file_lineage table entirely.")

if __name__ == "__main__":
    print("=========================================")
    print("   PropIntel Targeted Rollback Utility")
    print("=========================================")
    print("Options:")
    print("  gold   -> Wipes Iceberg tables (keeps Silver & Bronze intact)")
    print("  silver -> Wipes Silver parquet + lineage (keeps Bronze intact)")
    print("  bronze -> Wipes Bronze parquet + lineage (files return to landing zone)")
    print("  all    -> Nuclear Reset (wipes EVERYTHING)")
    print("=========================================")
    
    choice = input("Which layer do you want to reset? (gold/silver/bronze/all): ").strip().lower()
    
    if choice not in ['gold', 'silver', 'bronze', 'all']:
        print("Invalid choice. Exiting.")
        sys.exit(1)
        
    print(f"\nExecuting targeted rollback for: {choice.upper()}...")
    
    if choice in ['gold', 'all']:
        logger.info("Rolling back GOLD layer...")
        delete_iceberg_catalog(GOLD_ZONE)
        
    if choice in ['silver', 'all']:
        logger.info("Rolling back SILVER layer...")
        delete_parquet_files(SILVER_ZONE)
        reset_db_layer('SILVER')
        
    if choice in ['bronze', 'all']:
        logger.info("Rolling back BRONZE layer...")
        delete_parquet_files(BRONZE_ZONE)
        reset_db_layer('BRONZE')
        
    if choice == 'all':
        nuke_db()
        logger.info("Nuclear Reset Complete.")
    else:
        logger.info(f"Targeted rollback of '{choice}' complete.")
