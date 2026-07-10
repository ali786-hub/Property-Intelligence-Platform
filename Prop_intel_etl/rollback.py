"""
PropIntel Pipeline Rollback Utility.

Removes generated database records and files for specified pipeline layers
to facilitate clean state re-runs.
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
    raise ValueError("DATABASE_URL is not set in environment.")

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


def delete_files_in_directory(directory: str, pattern: str = "*") -> int:
    """
    Deletes all files matching a specific pattern inside a directory.
    
    Args:
        directory: Target folder path.
        pattern: Glob pattern to filter files.
    Returns:
        The count of successfully deleted files.
    """
    target_files = glob.glob(os.path.join(directory, pattern))
    deleted_count = 0
    for file_path in target_files:
        if os.path.isfile(file_path):
            os.remove(file_path)
            deleted_count += 1
    return deleted_count


def restore_archive_to_landing() -> int:
    """
    Moves all CSV files from the archive zone back to the landing zone.
    
    Returns:
        The number of files restored.
    """
    archived_files = glob.glob(os.path.join(ARCHIVE_ZONE, "*.csv"))
    restored_count = 0
    for file_path in archived_files:
        dest_path = os.path.join(LANDING_ZONE, os.path.basename(file_path))
        shutil.move(file_path, dest_path)
        restored_count += 1
    return restored_count


def wipe_neon_lineage(db_cursor, layer: str):
    """
    Deletes DB lineage records corresponding to a specific layer.
    
    Args:
        db_cursor: Active psycopg2 database cursor.
        layer: Layer to clean ('bronze', 'silver', 'gold', or 'all').
    """
    if layer == "all":
        # Cascade constraints automatically purge file_lineage when pipeline_runs rows are deleted.
        db_cursor.execute("DELETE FROM pipeline_runs;")
    else:
        db_cursor.execute("DELETE FROM file_lineage WHERE layer = %s;", (layer.upper(),))


def main():
    parser = argparse.ArgumentParser(description="PropIntel Rollback Tool")
    parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold", "all"],
                        help="The Medallion layer to rollback.")
    parser.add_argument("--restore", action="store_true",
                        help="Restore archived raw CSVs back to the landing folder.")
    args = parser.parse_args()

    print(f"{'='*50}")
    print(f"  PropIntel Rollback: {args.layer.upper()}")
    print(f"{'='*50}")

    conn = psycopg2.connect(NEON_CONN_STR)
    cur = conn.cursor()

    try:
        # Determine layers targeted for file deletion
        layers_to_clean = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]

        # 1. Clean files on disk
        for layer in layers_to_clean:
            layer_path = LAYER_PATHS[layer]
            if os.path.exists(layer_path):
                deleted_count = delete_files_in_directory(layer_path)
                print(f"  Wiped {deleted_count} files from {layer.upper()} zone.")
            else:
                print(f"  {layer.upper()} directory not found. Skipping.")

        # 2. Wipe DB records
        wipe_neon_lineage(cur, args.layer)
        conn.commit()
        print(f"  Neon DB state metadata for '{args.layer.upper()}' deleted.")

        # 3. Optional Landing Zone Restoration
        if args.restore:
            restored_count = restore_archive_to_landing()
            print(f"  Restored {restored_count} CSV(s) from archive back to landing zone.")

        print("\nRollback completed successfully. Workspace is reset.")

    except Exception as e:
        conn.rollback()
        print(f"Rollback failed: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
