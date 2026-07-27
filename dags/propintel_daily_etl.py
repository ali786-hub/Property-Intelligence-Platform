import sys
import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    description='Main ETL Pipeline: Bronze -> Silver -> Gold',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'etl'],
) as dag:

    # Task 1: Bronze Ingestion
    bronze_task = BashOperator(
        task_id='ingest_bronze',
        bash_command='/tmp/propintel_venv/bin/python /opt/airflow/src/ingestion/bronze_ingest.py',
        env={'AIRFLOW_RUN_ID': '{{ run_id }}', **os.environ},
    )

    # Task 2: Silver Transformation
    silver_task = BashOperator(
        task_id='transform_silver',
        bash_command='/tmp/propintel_venv/bin/python /opt/airflow/src/transformation/silver_transform.py',
        env={'AIRFLOW_RUN_ID': '{{ run_id }}', **os.environ},
    )

    # Task 3: Gold Publishing (Apache Iceberg)
    gold_task = BashOperator(
        task_id='load_gold_iceberg',
        bash_command='/tmp/propintel_venv/bin/python /opt/airflow/src/loading/gold_publish.py',
        env={'AIRFLOW_RUN_ID': '{{ run_id }}', **os.environ},
    )

    # Dependency Chain
    bronze_task >> silver_task >> gold_task
