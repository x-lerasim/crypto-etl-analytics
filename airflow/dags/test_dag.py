from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'apollo',
    'start_date': datetime(2026, 2, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id='test_hello_world',
    default_args=default_args,
    description="Простой тестовый DAG",
    schedule_interval=None,
    catchup=False,
    tags=['test'],
)

def print_hello():
    print("Hello from Airflow")
    return "Success"

task1 = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
    dag=dag,
)

task2 = BashOperator(
    task_id='check_folders',
    bash_command="ls -la /opt/airflow/",
    dag=dag
)

task1 >> task2