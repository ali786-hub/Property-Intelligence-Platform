"""
PropIntel Pipeline Tracker — The Pipeline's Brain.

This module is the ONLY place in the project that talks to the Neon DB.
ETL scripts (Bronze, Silver, Gold) never write raw SQL — they call
methods on this class instead.

Why this matters:
  - If you need to change the Neon schema, you change it HERE, not
    in every single ETL script.
  - ETL scripts stay focused on data transformation (their actual job).
  - Retry/quarantine logic lives in one place and applies everywhere.
  - The N+1 query problem is solved: we fetch ALL processed hashes in
    a single query, then filter locally in Python using a Set.

Usage (in my ETL scripts):
    with PipelineTracker('BRONZE') as tracker:
        eligible = tracker.get_eligible_files()
        for file_hash, file_name in eligible:
            tracker.pre_log(file_hash, file_name)
            # ... do your ETL work ...
            tracker.log_success(file_hash, row_count, file_size_bytes)
        # __exit__ automatically finalizes the run in Neon
"""

import os
import psycopg2
from dotenv import load_dotenv

# Load key-value pairs from .env file into environment variables
load_dotenv()

# ---------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------

# Maximum times a file can fail before being QUARANTINED.
# After this, the pipeline stops retrying it automatically.
# A human must inspect the file and either fix it or delete the
# lineage record to allow a fresh retry.
MAX_RETRIES = 3


# ---------------------------------------------------------------
# THE TRACKER CLASS
# ---------------------------------------------------------------

class PipelineTracker:
    """
    Manages all Neon DB interactions for one ETL run.

    Designed to be used as a context manager (with statement) so that
    the database connection and run finalization are always handled
    cleanly, even if the ETL script crashes mid-way.

    Args:
        layer (str): 'BRONZE', 'SILVER', or 'GOLD'
    """

    def __init__(self, layer: str):
        """
        Initialize the pipeline tracker for a specific Medallion layer.
        
        Args:
            layer (str): The layer name, must be 'BRONZE', 'SILVER', or 'GOLD'.
        """
        # Save the layer name in uppercase (e.g., 'BRONZE') as an instance variable
        self.layer = layer.upper()

        # Will store the database run ID once the run is started in Neon
        self.run_id = None

        # Database connection and cursor objects
        self.conn = None
        self.cur = None

        # Keep track of file counts during this run to determine the final run status
        self._success_count = 0
        self._fail_count = 0

        # Load the connection string from environment variables (.env)
        neon_conn_str = os.getenv("DATABASE_URL")
        if not neon_conn_str:
            raise ValueError("DATABASE_URL not found. Check your .env file.")
        self._conn_str = neon_conn_str

    # ---------------------------------------------------------------
    # Context Manager Interface (enables 'with PipelineTracker(...)' syntax)
    # ---------------------------------------------------------------

    def __enter__(self):
        """
        Called automatically by Python when entering the 'with' block.
        Opens the database connection and registers the run.
        """
        self._connect()
        self._start_run()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Called automatically by Python when the 'with' block exits.
        
        This handles two scenarios:
        1. Clean Exit: Runs to completion, we finalize the run as SUCCESS or PARTIAL_SUCCESS.
        2. Crash/Error: An exception occurred. We finalize the run as FAILED.
        
        Arguments passed by Python automatically if the block crashed:
            exception_type: The class of the error (e.g., ValueError)
            exception_value: The error message string (e.g., 'division by zero')
            exception_traceback: The detailed traceback object for debugging
        """
        if self.run_id is not None:
            # Scenario 1: The script crashed with an unhandled exception
            if exception_type is not None:
                self._finalize_run('FAILED')
            
            # Scenario 2: Script completed, but one or more individual files failed
            elif self._fail_count > 0:
                self._finalize_run('PARTIAL_SUCCESS')
            
            # Scenario 3: Script completed and all files succeeded
            else:
                self._finalize_run('SUCCESS')

        # Always close cursors and database connections to prevent memory leaks
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

        # Returning False tells Python NOT to suppress the error (let it propagate/crash normal execution)
        return False

    # ---------------------------------------------------------------
    # Public API — These are called by ETL scripts
    # ---------------------------------------------------------------

    def get_eligible_files(self) -> list[tuple[str, str]]:
        """
        Returns all files eligible for processing in this layer.

        For BRONZE:  Returns hashes of source CSVs not yet in Bronze (or
                     previously FAILED with retries remaining).
        For SILVER:  Returns Bronze-SUCCESS files that have no SILVER SUCCESS.
        For GOLD:    Returns Silver-SUCCESS files that have no GOLD SUCCESS.

        This is a SINGLE Neon query per run (not one-per-file).
        The caller passes in all candidate files and this method filters
        them against the fetched set.

        Returns:
            List of (file_hash, file_name) tuples ready for processing.
        """
        self._ensure_alive()

        # Bronze layer source files are raw CSVs sitting in the local directory.
        # The database doesn't know about local files beforehand, so eligibility
        # is determined by scanning the landing folder and querying get_processed_hashes().
        if self.layer == 'BRONZE':
            return []

        # Determine the previous layer name dynamically.
        # This maps the current layer to the layer that precedes it.
        # e.g., If current layer is 'SILVER', the previous layer is 'BRONZE'.
        layer_mappings = {
            'SILVER': 'BRONZE',
            'GOLD': 'SILVER'
        }
        previous_layer = layer_mappings[self.layer]

        # Query Neon: Find all files that are 'SUCCESS' in the previous layer,
        # but have not yet succeeded in this layer.
        self.cur.execute("""
            SELECT prev.file_hash, prev.file_name
            FROM file_lineage prev
            LEFT JOIN file_lineage curr
              ON prev.file_hash = curr.file_hash
              AND curr.layer = %s
              AND curr.status = 'SUCCESS'
            WHERE prev.layer = %s
              AND prev.status = 'SUCCESS'
              AND curr.file_hash IS NULL
        """, (self.layer, previous_layer))

        return self.cur.fetchall()

    def get_processed_hashes(self) -> set[str]:
        """
        SINGLE QUERY: Fetches ALL file hashes that have already been
        successfully processed in this layer.

        Used by Bronze to filter the landing zone without N+1 queries.
        Instead of asking Neon "is THIS file done?" for each file,
        we ask once: "give me ALL done files" and filter locally.

        Returns:
            A Python set of file hash strings (fast O(1) lookup).
        """
        self._ensure_alive()
        
        # We pass self.layer as a single-element tuple: (self.layer,)
        # Note the trailing comma: Python requires a trailing comma to distinguish 
        # a single-element tuple from a regular parenthesized expression.
        self.cur.execute("""
            SELECT file_hash FROM file_lineage
            WHERE layer = %s AND status = 'SUCCESS'
        """, (self.layer,))
        
        # Return as a set to allow instant O(1) lookups in Python
        return {row[0] for row in self.cur.fetchall()}

    def get_quarantined_hashes(self) -> set[str]:
        """
        Returns hashes of files that are QUARANTINED in this layer.
        ETL scripts use this to skip files permanently flagged as broken.
        """
        self._ensure_alive()
        self.cur.execute("""
            SELECT file_hash FROM file_lineage
            WHERE layer = %s AND status = 'QUARANTINED'
        """, (self.layer,))
        return {row[0] for row in self.cur.fetchall()}

    def pre_log(self, file_hash: str, file_name: str):
        """
        Logs a file as PROCESSING before ETL work begins.

        This is crash-safe: if the script dies mid-file, Neon already has a
        PROCESSING record. On the next run, the file will be detected as
        "not SUCCESS" and retried (up to MAX_RETRIES times).
        """
        self._ensure_alive()
        self.cur.execute("""
            INSERT INTO file_lineage (file_hash, layer, file_name, run_id, status)
            VALUES (%s, %s, %s, %s, 'PROCESSING')
            ON CONFLICT (file_hash, layer)
            DO UPDATE SET
                status   = 'PROCESSING',
                run_id   = EXCLUDED.run_id,
                updated_at = CURRENT_TIMESTAMP
        """, (file_hash, self.layer, file_name, self.run_id))
        self.conn.commit()

    def log_success(self, file_hash: str, row_count: int, file_size_bytes: int):
        """
        Marks a file as successfully processed. Records output metrics.
        """
        self._ensure_alive()
        self.cur.execute("""
            UPDATE file_lineage
            SET status          = 'SUCCESS',
                row_count       = %s,
                file_size_bytes = %s,
                error_message   = NULL,
                updated_at      = CURRENT_TIMESTAMP
            WHERE file_hash = %s AND layer = %s
        """, (row_count, file_size_bytes, file_hash, self.layer))
        self.conn.commit()
        self._success_count += 1

    def log_failure(self, file_hash: str, error_message: str):
        """
        Marks a file as FAILED and increments its retry counter.
        If retry_count reaches MAX_RETRIES, automatically QUARANTINES the file.
        """
        self._ensure_alive()

        # Increment retry count and check if we should quarantine
        self.cur.execute("""
            UPDATE file_lineage
            SET retry_count   = retry_count + 1,
                error_message = %s,
                updated_at    = CURRENT_TIMESTAMP
            WHERE file_hash = %s AND layer = %s
            RETURNING retry_count
        """, (error_message, file_hash, self.layer))

        result = self.cur.fetchone()
        new_retry_count = result[0] if result else 1

        if new_retry_count >= MAX_RETRIES:
            print(f"  [QUARANTINE] File has failed {new_retry_count} times. Quarantining.")
            self.cur.execute("""
                UPDATE file_lineage
                SET status = 'QUARANTINED', updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s AND layer = %s
            """, (file_hash, self.layer))
        else:
            self.cur.execute("""
                UPDATE file_lineage
                SET status = 'FAILED', updated_at = CURRENT_TIMESTAMP
                WHERE file_hash = %s AND layer = %s
            """, (file_hash, self.layer))

        self.conn.commit()
        self._fail_count += 1

    # ---------------------------------------------------------------
    # Private Helpers
    # ---------------------------------------------------------------

    def _connect(self):
        """Opens a fresh connection to Neon DB."""
        print(f"Connecting to Pipeline Brain (Neon)...")
        self.conn = psycopg2.connect(self._conn_str)
        self.cur = self.conn.cursor()

    def _ensure_alive(self):
        """
        Checks if the Neon connection is still alive.
        Neon is serverless and can drop idle connections.
        This is called before every query as a safety net.
        """
        try:
            self.cur.execute("SELECT 1;")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            print("  Neon connection dropped. Reconnecting...")
            self._connect()

    def _start_run(self):
        """Creates a new pipeline_runs record and stores the run_id."""
        self.cur.execute("""
            INSERT INTO pipeline_runs (layer_name, status)
            VALUES (%s, 'RUNNING')
            RETURNING run_id
        """, (self.layer,))
        self.run_id = self.cur.fetchone()[0]
        self.conn.commit()
        print(f"Started {self.layer} Run ID: {self.run_id}")

    def _finalize_run(self, final_status: str):
        """
        Updates the pipeline_runs record with the final status and summary counts.
        Called automatically by __exit__.
        """
        self._ensure_alive()
        self.cur.execute("""
            UPDATE pipeline_runs
            SET status          = %s,
                ended_at        = CURRENT_TIMESTAMP,
                files_processed = %s,
                files_failed    = %s
            WHERE run_id = %s
        """, (final_status, self._success_count, self._fail_count, self.run_id))
        self.conn.commit()
        print(f"\nRun {self.run_id} finalized: {final_status} "
              f"({self._success_count} succeeded, {self._fail_count} failed)")
