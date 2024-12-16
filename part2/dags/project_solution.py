from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 16),
    'retries': 1,
}

with DAG(
    'data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    landing_to_bronze = SparkSubmitOperator(
        task_id='loading_to_bronze',
        application='./Documents/test/loading_to_bronze.py',
        conn_id='spark_default'
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='./Documents/test/bronze_to_silver.py',
        conn_id='spark_default'
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        application='./Documents/test/silver_to_gold.py',
        conn_id='spark_default'
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
