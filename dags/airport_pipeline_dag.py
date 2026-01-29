from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'almog',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='airport_pipeline_dag',
    description='Airport data pipeline',
    schedule='@daily',  
    catchup=False,
) as dag:
    
    run_bronze_notebook = PapermillOperator(
        task_id="run_bronze_ingestion",
        input_nb="/opt/airflow/bronze_flights_api.ipynb",
        output_nb="/opt/airflow/logs/out-{{ ds }}.ipynb",
        parameters={"execution_date": "{{ ds }}"},
        kernel_name="python3"
    )