import os
import sys

# Dynamically add root project directory so we can import 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.helper_files.database import DBConnection

def init_database():
    """
    One-time setup script. Creates the file_lineage table in Azure PostgreSQL.
    Safe to run multiple times — IF NOT EXISTS prevents duplicates.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS file_lineage (
        id              SERIAL PRIMARY KEY,
        file_hash       VARCHAR(64) NOT NULL,
        layer           VARCHAR(20) NOT NULL,
        airflow_run_id  VARCHAR(100),
        file_name       VARCHAR(255) NOT NULL,
        status          VARCHAR(20) NOT NULL,
        row_count       BIGINT,
        file_size_bytes BIGINT,
        error_message   TEXT,
        retry_count     INTEGER DEFAULT 0,
        processed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (file_hash, layer)
    );

    CREATE INDEX IF NOT EXISTS idx_lineage_hash ON file_lineage(file_hash);
    CREATE INDEX IF NOT EXISTS idx_lineage_layer_status ON file_lineage(layer, status);
    """

    with DBConnection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            print("file_lineage table created successfully!")

if __name__ == "__main__":
    init_database()
