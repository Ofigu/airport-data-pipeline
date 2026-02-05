from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'almog',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='airport_pipeline',
    description='Airport data pipeline',
    schedule=None, #only manual trigger
    #catchup=True, #run backfill for past dates
    default_args=default_args,
    tags=['airport', 'flights', 'bronze'],
) as dag:

    bronze_ingestion = DatabricksSubmitRunOperator(
        task_id='bronze_ingestion',
        databricks_conn_id='databricks_default',  # Points to Airflow connection
        existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID'), 
        notebook_task={
            'notebook_path': os.getenv('BRONZE_PATH'),
            'base_parameters': {
                'execution_date': '{{ ds }}'
            },
        },
        polling_period_seconds=30,
        timeout_seconds=600,
    )

    silver_transformation = DatabricksSubmitRunOperator(
        task_id='silver_transformation',
        databricks_conn_id='databricks_default',  # Points to Airflow connection
        existing_cluster_id=os.getenv('DATABRICKS_CLUSTER_ID'),
        notebook_task={
            'notebook_path': os.getenv('SILVER_PATH'),
        },
        polling_period_seconds=30,
        timeout_seconds=600,
    )

    gold_modeling = BashOperator(
        task_id='gold_modeling',
        bash_command="""
        set -e  # Exit on any error
        cd /opt/airflow/airport_dbt
        /home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/config
        /home/airflow/.local/bin/dbt test --profiles-dir /opt/airflow/config
        """,
        env={
            **os.environ,  # Pass all environment variables
            'DATABRICKS_DBT_ACCESS_KEY': '{{ var.value.DATABRICKS_DBT_ACCESS_KEY }}',  # Use Airflow variable
        },
    )


bronze_ingestion >> silver_transformation >> gold_modeling
