import os
import sys
import duckdb
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.helper_files.iceberg_catalog import get_catalog

load_dotenv()

def verify_gold_layer():
    print("=========================================")
    print("      GOLD LAYER VERIFICATION SCRIPT")
    print("=========================================")
    
    catalog = get_catalog()
    
    # 1. Connect DuckDB and register Iceberg tables
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='2GB'")
    
    try:
        sales_table = catalog.load_table("propintel.gold_sales")
        rentals_table = catalog.load_table("propintel.gold_rentals")
        
        sales_arrow = sales_table.scan().to_arrow()
        rentals_arrow = rentals_table.scan().to_arrow()
        
        con.register('gold_sales', sales_arrow)
        con.register('gold_rentals', rentals_arrow)
        
        # Test 1: Row Counts
        print("\n[TEST 1] Row Counts (should roughly match the 71% / 28% split found in EDA)")
        sales_count = con.execute("SELECT COUNT(*) FROM gold_sales").fetchone()[0]
        rentals_count = con.execute("SELECT COUNT(*) FROM gold_rentals").fetchone()[0]
        print(f"Total Sales rows: {sales_count}")
        print(f"Total Rentals rows: {rentals_count}")
        
        # Test 2: Anomaly Flags
        print("\n[TEST 2] Anomaly Detection (Prices < 100,000 Sale or < 1,000 Rent)")
        sales_anomalies = con.execute("SELECT COUNT(*) FROM gold_sales WHERE is_anomaly = TRUE").fetchone()[0]
        rent_anomalies = con.execute("SELECT COUNT(*) FROM gold_rentals WHERE is_anomaly = TRUE").fetchone()[0]
        print(f"Sales anomalies detected: {sales_anomalies}")
        print(f"Rental anomalies detected: {rent_anomalies}")
        
        # Test 3: Plot/Land Category Mapping (0-bedrooms)
        print("\n[TEST 3] Property Category Mapping (0 bedrooms -> Plot/Land)")
        plot_count = con.execute("SELECT COUNT(*) FROM gold_sales WHERE property_category = 'Plot/Land' AND bedrooms = 0").fetchone()[0]
        print(f"0-bedroom records mapped to 'Plot/Land': {plot_count}")
        
        # Test 4: SCD Type 2 Duplicates Check
        print("\n[TEST 4] SCD Type 2 Integrity")
        print("Checking for duplicate active property_ids (Should be 0)...")
        sales_dupes = con.execute("""
            SELECT property_id, COUNT(*) as cnt 
            FROM gold_sales 
            WHERE is_current = TRUE 
            GROUP BY property_id 
            HAVING cnt > 1
        """).fetchall()
        print(f"Duplicate active Sales records: {len(sales_dupes)}")
        
        # Test 5: Date Fix Verification
        print("\n[TEST 5] Silver Date Fix Verification")
        sales_dates = con.execute("SELECT typeof(date_added) as type, COUNT(date_added) as valid_dates FROM gold_sales").fetchone()
        print(f"Column type is {sales_dates[0]}. Found {sales_dates[1]} valid dates.")
        
    except Exception as e:
        print(f"\n[ERROR] Verification failed: {e}")
        print("Make sure you have run the pipeline to populate the Gold Layer first.")

    con.close()
    print("=========================================")

if __name__ == "__main__":
    verify_gold_layer()
