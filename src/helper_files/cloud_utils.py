import os
import fsspec
import logging

def get_fs_and_options(path: str):
    """
    Returns the correct fsspec filesystem and storage options.
    If the path is an Azure URL, it automatically reads the credentials from .env.
    Otherwise, it returns the local filesystem.
    """
    storage_options = {}
    if path and (path.startswith("abfs://") or path.startswith("azure://")):
        storage_options = {
            "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
            "account_key": os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        }
    
    fs, _ = fsspec.core.url_to_fs(path, **storage_options)
    return fs, storage_options

def inject_duckdb_azure_secret(duckdb_conn):
    """
    Checks if Azure credentials exist in the environment.
    If so, loads the Azure extension and injects the credentials into DuckDB securely.
    """
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    
    if account_name and account_key:
        logging.info("Injecting Azure Cloud credentials into DuckDB...")
        duckdb_conn.execute("INSTALL azure;")
        duckdb_conn.execute("LOAD azure;")
        conn_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        duckdb_conn.execute(f"""
            CREATE SECRET IF NOT EXISTS azure_secret (
                TYPE AZURE,
                PROVIDER CONFIG,
                CONNECTION_STRING '{conn_string}'
            );
        """)
