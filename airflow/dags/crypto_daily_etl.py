from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_JOBS_PATH = '/opt/spark/jobs'
DBT_PROJECT_DIR = '/usr/app/crypto_analytics'  
DBT_PROFILES_DIR = '/home/dbt/.dbt'

default_args = {
    'owner': 'x-lerasim',
    'depends_on_past': False,
}

with DAG(
    dag_id='crypto_daily_etl',
    default_args=default_args,
    start_date=datetime(2026, 2, 15),
    schedule='0 9 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['crypto', 'spark', 'clickhouse'],
) as dag:
    
    ingest_bronze = SparkSubmitOperator(
        task_id='ingest_bronze',
        application=f'{SPARK_JOBS_PATH}/coincap_assets_brz.py',
        conn_id='spark_default',  
        conf={'spark.master': 'spark://spark-master:7077'},  
        application_args=['--execution-date', '{{ ds }}'],
        verbose=True,
    )

    process_silver = SparkSubmitOperator(
        task_id='process_silver',
        application=f'{SPARK_JOBS_PATH}/dim_assets_slv.py',
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
        application_args=['--execution-date', '{{ ds }}'],
        verbose=True,
    )

    load_clickhouse = SparkSubmitOperator(
        task_id='load_clickhouse',
        application=f'{SPARK_JOBS_PATH}/assets_to_clickhouse.py',
        conn_id='spark_default',
        conf={'spark.master': 'spark://spark-master:7077'},
        application_args=['--execution-date', '{{ ds }}'],
        verbose=True,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "docker exec dbt bash -lc "
            f"\"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}\""
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "docker exec dbt bash -lc "
            f"\"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} "
            "--select +marts\""
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "docker exec dbt bash -lc "
            f"\"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}\""
        ),
    )

    ingest_bronze >> process_silver >> load_clickhouse >> dbt_deps >> dbt_run >> dbt_test