from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks in this DAG
default_args = {
    'owner': 'propintel',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='hello_world_learning_dag',
    default_args=default_args,
    description='A simple DAG to learn the Airflow UI',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['learning'],
) as dag:

    def print_hello():
        print("Hello from Airflow! 🚀")
        print("If you are reading this in the UI logs, your Docker setup is perfect!")
        return "Task Completed"

    # Define a single Task using the PythonOperator
    hello_task = PythonOperator(
        task_id='say_hello_task',
        python_callable=print_hello,
    )

    # In a real DAG, we would chain tasks like this: task1 >> task2
    hello_task
