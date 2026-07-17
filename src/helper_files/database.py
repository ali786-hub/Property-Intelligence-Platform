import os
import logging
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

# Set up logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    logging.warning("DATABASE_URL environment variable is not set. Database connections will fail.")


# ---------------------------------------------------------------
# FIX 1: Lazy Pool Initialization
# ---------------------------------------------------------------
# The pool is no longer created at import time.
# It is only created the first time _get_pool() is called.
# This prevents the pipeline from crashing silently on startup
# if the database is not yet reachable (e.g., cold cloud deployments).

_connection_pool = None


def _get_pool() -> pool.SimpleConnectionPool:
    """
    Returns the global connection pool, initializing it on first call (lazy init).

    Raises:
        ConnectionError: If the pool cannot be created (e.g. bad credentials).
    """
    global _connection_pool

    if _connection_pool is None:
        try:
            _connection_pool = pool.SimpleConnectionPool(
                1, 10, dsn=DATABASE_URL
            )
            logging.info("PostgreSQL connection pool initialized successfully.")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize database connection pool: {e}")

    return _connection_pool


def get_db_connection():
    """
    Retrieves a connection from the global connection pool.

    Returns:
        psycopg2.connection: A live connection object.
    Raises:
        ConnectionError: If the pool is unavailable or exhausted.
    """
    try:
        return _get_pool().getconn()
    except Exception as e:
        raise ConnectionError(f"Failed to retrieve a connection from the pool: {e}")


def release_db_connection(conn):
    """
    Returns a connection back to the global connection pool.

    Args:
        conn: The psycopg2 connection object to return.
    """
    # Only attempt to return if a pool exists and the connection is not None
    if _connection_pool and conn:
        try:
            _connection_pool.putconn(conn)
        except Exception as e:
            logging.error(f"Error returning connection back to pool: {e}")


class DBConnection:
    """
    A context manager to handle database connections safely.
    Ensures the connection is returned to the pool even if errors occur.

    Usage:
        with DBConnection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    def __enter__(self):
        # FIX 2: Initialize self.conn to None BEFORE calling get_db_connection().
        # This guarantees that __exit__ always has a safe value to work with,
        # even if get_db_connection() crashes before assigning self.conn.
        self.conn = None
        self.conn = get_db_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # If an error occurred inside the 'with' block, rollback the transaction
            logging.error(f"Database transaction error: {exc_val}. Rolling back.")
            try:
                self.conn.rollback()
            except Exception as rollback_err:
                logging.error(f"Rollback failed: {rollback_err}")
        else:
            # Commit if no errors occurred
            try:
                self.conn.commit()
            except Exception as commit_err:
                logging.error(f"Commit failed: {commit_err}")

        # Always release the connection back to the pool (safe because self.conn = None was set first)
        release_db_connection(self.conn)
