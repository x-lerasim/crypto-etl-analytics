from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

SPARK_JOBS_PATH = '/opt/spark/jobs'

default_args = {
    'owner': 'x-lerasim',
    'depends_on_past': False,
}

with DAG(
    dag_id='crypto_daily_etl',
    default_args=default_args,
    start_date=datetime(2026, 2, 13),
    schedule='0 9 * * *',
    catchup=False,
    tags=['crypto', 'spark', 'clickhouse'],
) as dag:
    
    ingest_bronze = SparkSubmitOperator(
        task_id = 'ingest_bronze',
        application=f'{SPARK_JOBS_PATH}/coincap_assets_brz.py',
        conn_id='spark_default',
        arguments=['--execution-date', '{{ ds }}'],
        verbose=True,
    )

    process_silver = SparkSubmitOperator(
        task_id = 'process_silver',
        application=f'{SPARK_JOBS_PATH}/dim_assets_slv.py',
        conn_id='spark_default',
        arguments=['--execution-date', '{{ ds }}'],
        verbose=True,
    )

    load_clickhouse = SparkSubmitOperator(
        task_id  = 'load_clickhouse',
        application=f'{SPARK_JOBS_PATH}/assets_to_clickhouse.py',
        conn_id='spark_default',
        arguments=['--execution-date', '{{ ds }}'],
        verbose=True,
    )
    
    ingest_bronze >> process_silver >> load_clickhouse