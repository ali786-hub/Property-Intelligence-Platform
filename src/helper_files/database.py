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

# Establish a connection pool to handle multiple tasks or scale connections efficiently
# minconn=1, maxconn=10 is plenty for our local and orchestrator usage
try:
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10, dsn=DATABASE_URL
    )
    logging.info("PostgreSQL connection pool initialized successfully.")
except Exception as e:
    connection_pool = None
    logging.error(f"Failed to initialize database connection pool: {e}")


def get_db_connection():
    """
    Retrieves a connection from the global connection pool.
    
    Returns:
        psycopg2.connection: A connection object to interact with the database.
    Raises:
        ConnectionError: If the connection pool is not initialized or fails.
    """
    if not connection_pool:
        raise ConnectionError("Database connection pool is not initialized. Check your DATABASE_URL.")
    
    try:
        return connection_pool.getconn()
    except Exception as e:
        raise ConnectionError(f"Failed to retrieve a connection from the pool: {e}")


def release_db_connection(conn):
    """
    Returns a connection back to the global connection pool.
    
    Args:
        conn: The psycopg2 connection object to return.
    """
    if connection_pool and conn:
        try:
            connection_pool.putconn(conn)
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
                
        # Always release the connection back to the pool
        release_db_connection(self.conn)
