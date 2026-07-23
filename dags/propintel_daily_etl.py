import sys
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# =================================================================
# PATH INJECTION
# Airflow runs in /opt/airflow. Our custom modules are in /opt/airflow/src.
# We must inject this into sys.path so Python can find our code.
# =================================================================
sys.path.insert(0, '/opt/airflow')

# Now we can safely import our pipeline functions
from src.ingestion.bronze_ingest import ingest_to_bronze
from src.transformation.silver_transform import transform_to_silver
from src.helper_files.database import DBConnection

# Set up logging for the DAG file itself
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =================================================================
# AIRFLOW WRAPPER FUNCTIONS
# Airflow injects a 'context' dictionary into tasks via **kwargs.
# We extract 'run_id' to pass it down to our LineageTracker!
# =================================================================

def run_bronze_layer(**kwargs):
    run_id = kwargs['run_id']
    logger.info(f"Starting Bronze Layer for Airflow Run: {run_id}")
    
    # We pass the airflow_run_id so it gets logged in the PostgreSQL lineage table
    ingest_to_bronze(airflow_run_id=run_id)
    
    logger.info("Bronze Layer completed successfully.")
    return "Bronze Done"

def run_silver_layer(**kwargs):
    run_id = kwargs['run_id']
    logger.info(f"Starting Silver Layer for Airflow Run: {run_id}")
    
    transform_to_silver(airflow_run_id=run_id)
    
    logger.info("Silver Layer completed successfully.")
    return "Silver Done"


# =================================================================
# DAG DEFINITION
# =================================================================

default_args = {
    'owner': 'propintel',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='propintel_daily_etl',
    default_args=default_args,
    description='Main ETL Pipeline: Bronze -> Silver',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['production', 'etl'],
) as dag:

    # Task 1: Bronze Ingestion
    bronze_task = PythonOperator(
        task_id='ingest_bronze',
        python_callable=run_bronze_layer,
        provide_context=True, # Critical: This tells Airflow to pass **kwargs
    )

    # Task 2: Silver Transformation
    silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=run_silver_layer,
        provide_context=True,
    )

    # Dependency Chain
    bronze_task >> silver_task
