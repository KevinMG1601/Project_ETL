from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Agrega la raíz del proyecto al sys.path para que se reconozca src
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

# Importar funciones desde etl
from tasks.etl import extract_task, transform_task, load_task, validate_task

default_args = {
    'owner': 'kevinmg',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='accidents_etl_pipeline',
    description='ETL pipeline for Traffic Accidents dataset',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 5),
    catchup=False,
    tags=['accidents', 'ETL'],
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_task
    )

    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_task
    )

    extract >> transform >> load >> validate
