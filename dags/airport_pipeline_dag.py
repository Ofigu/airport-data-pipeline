from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
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
    schedule='none', #only manual trigger
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


bronze_ingestion >> silver_transformation
