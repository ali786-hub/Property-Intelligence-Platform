from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'propintel',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    'propintel_reset_silver',
    default_args=default_args,
    description='Teardown DAG: Wipes Silver zone to roll back pipeline state to Bronze.',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['utility', 'admin', 'reset'],
) as dag:

    # We use BashOperator to run the python script using our isolated Virtual Environment
    # This guarantees PyIceberg and DuckDB have access to the adlfs/azure packages
    reset_task = BashOperator(
        task_id='reset_silver_layer',
        bash_command='/tmp/propintel_venv/bin/python /opt/airflow/src/utils/reset_silver.py',
    )
