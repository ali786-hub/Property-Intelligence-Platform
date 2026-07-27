import os
import sys
import duckdb
import io
from dotenv import load_dotenv

# Force UTF-8 encoding for Windows terminals to support emojis
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

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
        print("\n⏳ Bulk-downloading Gold Sales table from Azure into PyArrow memory... (takes 5-15 seconds)")
        sales_table = catalog.load_table("propintel.gold_sales")
        sales_arrow = sales_table.scan().to_arrow()
        con.register('gold_sales', sales_arrow)
        print(f"✅ Sales downloaded. Total Rows: {len(sales_arrow):,}")
        
        print("\n⏳ Bulk-downloading Gold Rentals table from Azure into PyArrow memory... (takes 5-15 seconds)")
        rentals_table = catalog.load_table("propintel.gold_rentals")
        rentals_arrow = rentals_table.scan().to_arrow()
        con.register('gold_rentals', rentals_arrow)
        print(f"✅ Rentals downloaded. Total Rows: {len(rentals_arrow):,}")
        
        # Test 1: Row Counts
        print("\n[TEST 1] Row Counts (should roughly match the 71% / 28% split found in EDA)")
        sales_count = con.execute("SELECT COUNT(*) FROM gold_sales").fetchone()[0]
        rentals_count = con.execute("SELECT COUNT(*) FROM gold_rentals").fetchone()[0]
        print(f"Total Sales rows: {sales_count:,}")
        print(f"Total Rentals rows: {rentals_count:,}")
        
        # Test 1.5: Strict Null Check
        print("\n[TEST 1.5] Strict NULL checks on Foundation Columns (Should all be 0)")
        null_query = """
            SELECT 
                COUNT(*) FILTER (WHERE property_id IS NULL) as null_id,
                COUNT(*) FILTER (WHERE url_hash IS NULL) as null_hash,
                COUNT(*) FILTER (WHERE price IS NULL) as null_price,
                COUNT(*) FILTER (WHERE area_marla IS NULL) as null_area
            FROM gold_sales
        """
        nulls = con.execute(null_query).fetchdf().iloc[0]
        for col, val in nulls.items():
            status = "✅" if val == 0 else "❌"
            print(f"  {status} {col}: {val}")
            
        # Test 2: Anomaly Flags
        print("\n[TEST 2] Anomaly Detection (Prices < 100,000 Sale or < 1,000 Rent)")
        sales_anomalies = con.execute("SELECT COUNT(*) FROM gold_sales WHERE is_anomaly = TRUE").fetchone()[0]
        rent_anomalies = con.execute("SELECT COUNT(*) FROM gold_rentals WHERE is_anomaly = TRUE").fetchone()[0]
        print(f"Sales anomalies detected: {sales_anomalies:,}")
        print(f"Rental anomalies detected: {rent_anomalies:,}")
        
        # Test 3: Plot/Land Category Mapping (0-bedrooms)
        print("\n[TEST 3] Property Category Mapping (0 bedrooms -> Plot/Land)")
        plot_count = con.execute("SELECT COUNT(*) FROM gold_sales WHERE property_category = 'Plot/Land' AND bedrooms = 0").fetchone()[0]
        print(f"0-bedroom records mapped to 'Plot/Land': {plot_count}")
        
        # Test 4: SCD Type 2 Duplicates Check
        print("\n[TEST 4] SCD Type 2 Integrity (Current Records)")
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
        sales_dates = con.execute("SELECT typeof(date_added) as type, COUNT(date_added) as valid_dates FROM gold_sales GROUP BY typeof(date_added)").fetchone()
        print(f"Column type is {sales_dates[0]}. Found {sales_dates[1]:,} valid dates.")
        
        # Test 6: SCD2 Price History (proves the SCD2 timeline actually works)
        print("\n[TEST 6] SCD Type 2 Price History Validation")
        expired_records = con.execute("SELECT COUNT(*) FROM gold_sales WHERE is_current = FALSE").fetchone()[0]
        print(f"Total historical (expired) versions retained: {expired_records:,}")
        
        properties_with_history = con.execute("""
            SELECT COUNT(DISTINCT property_id) FROM gold_sales WHERE is_current = FALSE
        """).fetchone()[0]
        print(f"Total unique properties that had price changes over the 10 days: {properties_with_history:,}")
        
        # Show a sample property's timeline if any exist
        if properties_with_history > 0:
            sample = con.execute("""
                SELECT property_id, price, is_current, valid_from, valid_to
                FROM gold_sales
                WHERE property_id IN (
                    SELECT property_id FROM gold_sales GROUP BY property_id HAVING COUNT(*) > 1 LIMIT 1
                )
                ORDER BY valid_from
            """).fetchdf()
            print("\nSample SCD2 Property Timeline (Look at how valid_from/valid_to chain together!):")
            print(sample.to_string(index=False))
        else:
            print("No price changes detected across the 10 days (possible with synthetic data).")
        
        # Test 7: City Distribution
        print("\n[TEST 7] City Distribution")
        cities = con.execute("""
            SELECT city, COUNT(*) as total_rows, 
                   SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as active_rows
            FROM gold_sales GROUP BY city ORDER BY total_rows DESC
        """).fetchdf()
        print(cities.to_string(index=False))
        
        # Test 8: Iceberg Snapshot Tracking
        print("\n[TEST 8] Apache Iceberg Snapshot Verification")
        print("Proving that Iceberg is correctly tracking table history and time-travel versions...")
        snapshots = sales_table.snapshots()
        if not snapshots:
            print("❌ No Iceberg snapshots found!")
        else:
            print(f"✅ Found {len(snapshots)} Iceberg Snapshots!")
            from datetime import datetime
            for snap in snapshots[-5:]: # Show up to the last 5 snapshots
                dt = datetime.fromtimestamp(snap.timestamp_ms / 1000.0)
                print(f"  - Snapshot ID: {snap.snapshot_id} | Created At: {dt}")
        
        # Test 9: Export Full SCD2 History
        print("\n[TEST 9] Exporting Full SCD Type 2 Timeline to CSV")
        if properties_with_history > 0:
            export_query = """
            SELECT 
                property_id, city, location, property_type, price, is_current, valid_from, valid_to
            FROM gold_sales
            WHERE property_id IN (
                SELECT property_id FROM gold_sales GROUP BY property_id HAVING COUNT(*) > 1
            )
            ORDER BY city, property_id, valid_from
            """
            print("Extracting full SCD2 history for all records...")
            df = con.execute(export_query).fetchdf()
            
            output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../SCD2_Full_Timeline.csv'))
            df.to_csv(output_file, index=False)
            print(f"✅ SUCCESS! {len(df):,} historical records exported.")
            print(f"Open this file in Excel to see the exact price history timeline for every property: {output_file}")
        else:
            print("No history to export yet.")
            
        # Test 10: Deep Observation (Top 2 SCD2 Timelines)
        print("\n[TEST 10] SCD Type 2 - Deep Observation")
        print("Finding 2 properties that actually changed their price multiple times...")
        print("Note: In SCD Type 2, if a price does NOT change for 5 days, it does NOT create 5 rows!")
        print("Instead, it creates ONE row with a valid_to date spanning those 5 days. This saves millions of rows of space.")
        
        if properties_with_history > 0:
            top_props = con.execute("""
                SELECT property_id, COUNT(*) as changes
                FROM gold_sales 
                GROUP BY property_id 
                ORDER BY changes DESC 
                LIMIT 2
            """).fetchall()
            
            for row in top_props:
                prop_id = row[0]
                changes = row[1]
                print(f"\n🏠 PROPERTY ID: {prop_id} (Changed price {changes} times over the 21 days!)")
                print("-" * 80)
                history = con.execute(f"""
                    SELECT valid_from, valid_to, is_current, price, city, location
                    FROM gold_sales
                    WHERE property_id = {prop_id}
                    ORDER BY valid_from
                """).fetchdf()
                print(history.to_string(index=False))
                print("-" * 80)
        else:
            print("No history to observe yet.")
            
    except Exception as e:
        print(f"\n[ERROR] Verification failed: {e}")
        import traceback
        traceback.print_exc()

    con.close()
    print("\n=========================================")

if __name__ == "__main__":
    verify_gold_layer()
