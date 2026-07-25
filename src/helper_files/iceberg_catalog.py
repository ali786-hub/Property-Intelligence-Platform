import os
from pyiceberg.catalog.sql import SqlCatalog
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
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform

def get_catalog():
    """
    Initializes and returns the PyIceberg SQL catalog pointing to the PostgreSQL DB.
    Creates the necessary directories for the warehouse if they don't exist.
    """
    gold_zone = os.getenv("GOLD_ZONE", "C:/Omnijourney_Kofking_github/data/gold")
    warehouse_path = os.path.join(gold_zone, "warehouse").replace("\\", "/")
    
    # Ensure local warehouse directories exist
    os.makedirs(warehouse_path, exist_ok=True)
    
    # Get DB credentials from .env
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    
    # Construct PostgreSQL URI (psycopg2 is standard for sqlalchemy with postgres)
    catalog_uri = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Initialize PyIceberg catalog pointing to the central PostgreSQL DB
    catalog = SqlCatalog(
        "default",
        **{
            "uri": catalog_uri,
            "warehouse": warehouse_path,
        },
    )
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
