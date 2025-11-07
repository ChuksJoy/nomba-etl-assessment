from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Joy',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='test_dag',
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['test'],
) as dag:
    
    task1 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello Joy, Airflow is running successfully!"'
    )

    task1
