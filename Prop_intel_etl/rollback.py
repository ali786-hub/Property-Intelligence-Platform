"""
PropIntel Pipeline Rollback Utility.

Cleans data and Neon DB lineage records for a specific layer (or all layers),
allowing you to re-test the pipeline from a clean state.

Usage:
    python rollback.py --layer bronze              # Wipe Bronze data + lineage
    python rollback.py --layer silver              # Wipe Silver data + lineage
    python rollback.py --layer all                 # Wipe everything
    python rollback.py --layer bronze --restore    # Wipe Bronze AND move archived CSVs back to landing
    python rollback.py --layer all --restore       # Full factory reset
"""

import os
import glob
import shutil
import argparse
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
NEON_CONN_STR = os.getenv("DATABASE_URL")
if not NEON_CONN_STR:
    raise ValueError("CRITICAL ERROR: Python cannot find DATABASE_URL. Check your .env file!")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
WORKSPACE_ROOT = os.path.dirname(PROJECT_ROOT)

LANDING_ZONE = os.path.join(WORKSPACE_ROOT, "data", "landing_zone")
ARCHIVE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "archive_zone")
BRONZE_ZONE = os.path.join(WORKSPACE_ROOT, "data", "bronze")
SILVER_ZONE = os.path.join(WORKSPACE_ROOT, "data", "silver")
GOLD_ZONE = os.path.join(WORKSPACE_ROOT, "data", "gold")

LAYER_PATHS = {
    "bronze": BRONZE_ZONE,
    "silver": SILVER_ZONE,
    "gold": GOLD_ZONE,
}


def delete_files_in_directory(directory, pattern="*"):
    """Deletes all files matching pattern in the given directory. Returns count of deleted files."""
    files = glob.glob(os.path.join(directory, pattern))
    count = 0
    for f in files:
        if os.path.isfile(f):
            os.remove(f)
            count += 1
    return count


def restore_archive_to_landing():
    """Moves all CSV files from archive_zone back to landing_zone."""
    archived_files = glob.glob(os.path.join(ARCHIVE_ZONE, "*.csv"))
    count = 0
    for f in archived_files:
        dest = os.path.join(LANDING_ZONE, os.path.basename(f))
        shutil.move(f, dest)
        count += 1
    return count


def wipe_neon_lineage(cur, layer):
    """Deletes file_lineage records for the specified layer."""
    if layer == "all":
        cur.execute("DELETE FROM file_lineage;")
        cur.execute("DELETE FROM pipeline_runs;")
    else:
        cur.execute("DELETE FROM file_lineage WHERE layer = %s;", (layer.upper(),))


def main():
    parser = argparse.ArgumentParser(description="PropIntel Pipeline Rollback Utility")
    parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold", "all"],
                        help="Which layer to rollback.")
    parser.add_argument("--restore", action="store_true",
                        help="Move archived CSVs back to landing zone (only relevant for bronze/all rollback).")
    args = parser.parse_args()

    print(f"{'='*50}")
    print(f"  PropIntel Rollback: {args.layer.upper()}")
    print(f"{'='*50}")

    # 1. Connect to Neon
    conn = psycopg2.connect(NEON_CONN_STR)
    cur = conn.cursor()

    # 2. Determine which layers to clean
    if args.layer == "all":
        layers_to_clean = ["bronze", "silver", "gold"]
    else:
        layers_to_clean = [args.layer]

    # 3. Delete data files for each target layer
    for layer in layers_to_clean:
        layer_path = LAYER_PATHS[layer]
        if os.path.exists(layer_path):
            deleted = delete_files_in_directory(layer_path)
            print(f"  Deleted {deleted} files from {layer.upper()} ({layer_path})")
        else:
            print(f"  {layer.upper()} directory does not exist. Skipping file cleanup.")

    # 4. Wipe Neon lineage records
    wipe_neon_lineage(cur, args.layer)
    conn.commit()
    print(f"  Neon DB lineage records for '{args.layer.upper()}' wiped.")

    # 5. Optionally restore archived CSVs to landing zone
    if args.restore:
        restored = restore_archive_to_landing()
        print(f"  Restored {restored} CSV files from archive back to landing zone.")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nRollback complete. Pipeline is ready for re-testing.")


if __name__ == "__main__":
    main()
