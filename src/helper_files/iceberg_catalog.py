import os

# ---------------------------------------------------------
# PYICEBERG CORE IMPORTS
# ---------------------------------------------------------

# SqlCatalog: The core class that acts as our "Vault Architect".
# This connects to Azure PostgreSQL to manage the 'iceberg_tables' pointer.
from pyiceberg.catalog.sql import SqlCatalog

# Schema & Types: Used to define the exact physical shape of our tables.
# Iceberg requires strict types (e.g., LongType, StringType) so that
# query engines know exactly what data format to expect in the Parquet files.
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType,
    IntegerType,
    StringType,
    DoubleType,
    BooleanType,
    DateType,
    NestedField,
)

# Partitioning: The structural logic for creating folders on the hard drive.
# PartitionSpec: Defines the overall rule (e.g., "Group data by City").
# PartitionField: Defines the specific column to group by.
# IdentityTransform: Tells Iceberg to use the exact string name (e.g., 'Lahore') 
# for the folder name (city=Lahore) instead of hashing or truncating it.
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

def get_catalog():
    """
    Initializes and returns the PyIceberg SQL catalog pointing to the PostgreSQL DB.
    Creates the necessary directories for the warehouse if they don't exist.
    """
    gold_zone = os.getenv("GOLD_ZONE", "C:/Omnijourney_Kofking_github/data/gold")
    
    # Safely construct the warehouse path without os.path.join which can inject backslashes
    if gold_zone.endswith("/"):
        local_warehouse_path = f"{gold_zone}warehouse"
    else:
        local_warehouse_path = f"{gold_zone}/warehouse"
    
    # Ensure local warehouse directories exist ONLY if it's a local path
    if not (local_warehouse_path.startswith("abfs://") or local_warehouse_path.startswith("azure://")):
        os.makedirs(local_warehouse_path, exist_ok=True)
        
        # PyIceberg on Windows crashes if it sees "file:///C:/". 
        # Safely strip the drive letter (C:) for local Windows paths.
        if local_warehouse_path[1] == ':':
            iceberg_warehouse_uri = local_warehouse_path[2:]
        else:
            iceberg_warehouse_uri = local_warehouse_path
    else:
        # If it is a cloud path, we pass it exactly as-is
        iceberg_warehouse_uri = local_warehouse_path
    
    from urllib.parse import quote_plus
    
    # Get DB credentials from .env
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    
    # URL-encode credentials to safely handle special characters like '@', '#', or ':'
    user_encoded = quote_plus(db_user) if db_user else ""
    pass_encoded = quote_plus(db_password) if db_password else ""

    # Construct PostgreSQL URI (psycopg2 is standard for sqlalchemy with postgres)
    postgres_conn = f"postgresql+psycopg2://{user_encoded}:{pass_encoded}@{db_host}:{db_port}/{db_name}"

    # Initialize kwargs for the catalog
    catalog_kwargs = {
        "uri": postgres_conn,
        "warehouse": iceberg_warehouse_uri,
    }

    # If the warehouse is in Azure ADLS Gen2, PyIceberg needs the credentials
    if local_warehouse_path.startswith("abfs://") or local_warehouse_path.startswith("azure://"):
        account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        if account_name and account_key:
            catalog_kwargs["adls.account-name"] = account_name
            catalog_kwargs["adls.account-key"] = account_key
        else:
            logging.warning("Warehouse path is Azure, but AZURE_STORAGE_ACCOUNT_NAME or KEY is missing from .env")

    catalog = SqlCatalog("default", **catalog_kwargs)
    return catalog

def get_gold_schema() -> Schema:
    """
    Returns the Iceberg Schema for the Gold tables (both sales and rentals).
    This schema defines the columns for the One Big Table (OBT) with SCD Type 2 tracking.
    """
    return Schema(
        NestedField(field_id=1, name="property_id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="location_id", field_type=IntegerType(), required=False),
        NestedField(field_id=3, name="page_url", field_type=StringType(), required=False),
        NestedField(field_id=4, name="url_hash", field_type=StringType(), required=False),
        NestedField(field_id=5, name="property_type", field_type=StringType(), required=False),
        NestedField(field_id=6, name="property_category", field_type=StringType(), required=False), # New field
        NestedField(field_id=7, name="price", field_type=LongType(), required=False),
        NestedField(field_id=8, name="price_per_marla", field_type=DoubleType(), required=False),
        NestedField(field_id=9, name="is_anomaly", field_type=BooleanType(), required=False), # New field
        NestedField(field_id=10, name="location", field_type=StringType(), required=False),
        NestedField(field_id=11, name="city", field_type=StringType(), required=False),
        NestedField(field_id=12, name="province_name", field_type=StringType(), required=False),
        NestedField(field_id=13, name="latitude", field_type=DoubleType(), required=False),
        NestedField(field_id=14, name="longitude", field_type=DoubleType(), required=False),
        NestedField(field_id=15, name="baths", field_type=IntegerType(), required=False),
        NestedField(field_id=16, name="bedrooms", field_type=IntegerType(), required=False),
        NestedField(field_id=17, name="agency", field_type=StringType(), required=False),
        NestedField(field_id=18, name="agent", field_type=StringType(), required=False),
        NestedField(field_id=19, name="area_marla", field_type=DoubleType(), required=False),
        NestedField(field_id=20, name="date_added", field_type=DateType(), required=False),
        
        # SCD Type 2 Fields
        NestedField(field_id=21, name="is_current", field_type=BooleanType(), required=False),
        NestedField(field_id=22, name="valid_from", field_type=DateType(), required=False),
        NestedField(field_id=23, name="valid_to", field_type=DateType(), required=False),
        
        # Audit Fields
        NestedField(field_id=24, name="_ingested_at", field_type=StringType(), required=False),
        NestedField(field_id=25, name="_bronze_airflow_run_id", field_type=StringType(), required=False),
        NestedField(field_id=26, name="_transformed_at", field_type=StringType(), required=False),
        NestedField(field_id=27, name="_silver_airflow_run_id", field_type=StringType(), required=False),
        NestedField(field_id=28, name="_gold_loaded_at", field_type=StringType(), required=False),
    )

def get_gold_partition_spec() -> PartitionSpec:
    """
    Returns the Partition Specification for the Gold tables.
    We partition by city so queries filtering by city can skip irrelevant files.
    """
    return PartitionSpec(
        PartitionField(source_id=11, field_id=1000, transform=IdentityTransform(), name="city")
    )

def ensure_tables_exist(catalog):
    """
    Checks if the gold_sales and gold_rentals tables exist, and creates them if they don't.
    """
    schema = get_gold_schema()
    partition_spec = get_gold_partition_spec()
    
    # Iceberg requires namespaces (like databases/schemas). We'll use a 'propintel' namespace.
    try:
        catalog.create_namespace("propintel")
    except Exception:
        pass # Namespace likely already exists
        
    # Ensure Sales table
    try:
        catalog.load_table("propintel.gold_sales")
    except Exception:
        catalog.create_table(
            identifier="propintel.gold_sales",
            schema=schema,
            partition_spec=partition_spec,
        )
        print("Created Iceberg table: propintel.gold_sales")
        
    # Ensure Rentals table
    try:
        catalog.load_table("propintel.gold_rentals")
    except Exception:
        catalog.create_table(
            identifier="propintel.gold_rentals",
            schema=schema,
            partition_spec=partition_spec,
        )
        print("Created Iceberg table: propintel.gold_rentals")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    catalog = get_catalog()
    ensure_tables_exist(catalog)
    print("Catalog initialization test complete.")
