# dags/airport_pipeline_dag.py
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'almog',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='airport_pipeline',
    description='Airport data pipeline - Bronze layer ingestion',
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=['airport', 'flights', 'bronze'],
) as dag:
    
    bronze_ingestion = DatabricksSubmitRunOperator(
        task_id='bronze_ingestion',
        databricks_conn_id='databricks_default',
        notebook_task={
            'notebook_path': '/Workspace/Users/kseniakor30@gmail.com/airport-data-pipeline/bronze_flights_api',
            'base_parameters': {
                'execution_date': '{{ ds }}'
            }
        },
        new_cluster={
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'i3.xlarge',
            'num_workers': 2,
            'spark_conf': {
                'spark.databricks.delta.preview.enabled': 'true'
            }
        }
    )