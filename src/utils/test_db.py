import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.helper_files.database import DBConnection

def explore_lineage():
    """Prints a formatted overview of the file_lineage table in Azure PostgreSQL."""

    with DBConnection() as conn:
        with conn.cursor() as cur:

            # 1. Total records per layer and status
            print("=" * 70)
            print("  PIPELINE ANALYTICS: file_lineage table")
            print("=" * 70)

            cur.execute("SELECT layer, status, COUNT(*) FROM file_lineage GROUP BY layer, status ORDER BY layer, status;")
            rows = cur.fetchall()
            print("\n📊 Records by Layer & Status:")
            print(f"  {'Layer':<12} {'Status':<12} {'Count':<10}")
            print(f"  {'-'*12} {'-'*12} {'-'*10}")
            for layer, status, count in rows:
                print(f"  {layer:<12} {status:<12} {count:<10}")

            # 2. Total rows ingested per layer
            cur.execute("SELECT layer, SUM(row_count) as total_rows, SUM(file_size_bytes) as total_bytes FROM file_lineage WHERE status='SUCCESS' GROUP BY layer;")
            rows = cur.fetchall()
            print("\n📦 Data Volume by Layer:")
            print(f"  {'Layer':<12} {'Total Rows':<18} {'Total Size (MB)':<18}")
            print(f"  {'-'*12} {'-'*18} {'-'*18}")
            for layer, total_rows, total_bytes in rows:
                mb = round(float(total_bytes) / 1e6, 1) if total_bytes else 0
                print(f"  {layer:<12} {total_rows:<18,} {mb:<18}")

            # 3. Show all individual file records
            cur.execute("SELECT file_name, layer, status, row_count, file_size_bytes, processed_at FROM file_lineage ORDER BY layer, processed_at;")
            rows = cur.fetchall()
            print(f"\n📋 All {len(rows)} Lineage Records:")
            print(f"  {'File Name':<45} {'Layer':<10} {'Status':<10} {'Rows':<12} {'Size (MB)':<12} {'Processed At'}")
            print(f"  {'-'*45} {'-'*10} {'-'*10} {'-'*12} {'-'*12} {'-'*20}")
            for name, layer, status, rows_count, size, ts in rows:
                mb = round(size / 1e6, 1) if size else "—"
                rc = f"{rows_count:,}" if rows_count else "—"
                print(f"  {name:<45} {layer:<10} {status:<10} {rc:<12} {str(mb):<12} {ts}")

            # 4. Check for any FAILED records
            cur.execute("SELECT file_name, layer, error_message, retry_count FROM file_lineage WHERE status='FAILED';")
            failed = cur.fetchall()
            if failed:
                print(f"\n🚨 {len(failed)} FAILED Record(s):")
                for name, layer, err, retries in failed:
                    print(f"  [{layer}] {name} (retries: {retries})")
                    print(f"    Error: {err}")
            else:
                print("\n✅ No failed records found. Pipeline is healthy!")

            print("\n" + "=" * 70)

if __name__ == "__main__":
    explore_lineage()
