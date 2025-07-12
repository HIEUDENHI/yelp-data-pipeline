from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['hieudenhi.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='yelp_delta_pipeline_monthly',
    default_args=default_args,
    description='Monthly bronze→silver→gold pipeline',
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver_monthly',
        application='/opt/scripts/bronze_to_silver_on_minio.py',
        conn_id='spark_default',
        deploy_mode='client',
        application_args=[
            '--bronze-base', 's3a://yelp-data/bronze',
            '--silver-base', 's3a://yelp-data/silver',
            '--year',  "{{ dag_run.conf.get('year') or execution_date.strftime('%Y') }}",
            '--month', "{{ dag_run.conf.get('month') or execution_date.strftime('%m') }}",
        ]
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold_monthly',
        application='/opt/scripts/silver_to_gold_on_minio.py',
        conn_id='spark_default',
        deploy_mode='client',
        application_args=[
            '--silver-base', 's3a://yelp-data/silver',
            '--gold-base',   's3a://yelp-data/gold',
            '--year',  "{{ dag_run.conf.get('year') or execution_date.strftime('%Y') }}",
            '--month', "{{ dag_run.conf.get('month') or execution_date.strftime('%m') }}",
        ]
    )

    bronze_to_silver >> silver_to_gold
