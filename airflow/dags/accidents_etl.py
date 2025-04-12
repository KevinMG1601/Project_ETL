from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import os
from airflow.models import Variable

Variable.set("KAGGLE_DATA_PATH", "/home/kevin/github/Project_ETL/data/traffic_accidents.csv")
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)
from tasks.etl import (
    check_conn_task,
    extract_kaggle_task,
    extract_task,
    transform_csv_task,
    load_task,
    transform_task,
    extract_clean_task,
    check_model_task,
    create_model_task,
    load_model_data_task
)

default_args = {
    'owner': 'kevinmg',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='accidents_etl',
    description='ETL pipeline for Traffic Accidents dataset',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 5),
    catchup=False,
    tags=['accidents', 'ETL'],
) as dag:
    check_connection = PythonOperator(
        task_id='check_connection',
        python_callable=check_conn_task
    )
    
    extract_data_kaggle_csv = PythonOperator(
        task_id='extract_kaggle_data',
        python_callable=extract_kaggle_task
    )

    transform_and_load_csv = PythonOperator(
        task_id='transform_and_load_csv',
        python_callable=transform_csv_task
    )
    
    extrac_data_db_raw = PythonOperator(
        task_id='extract_data_raw',
        python_callable=extract_task
    )
    
    transform_data_db_raw = PythonOperator(
        task_id='transform_data',
        python_callable=transform_task
    )
    
    load_data_db_raw = PythonOperator(
        task_id='load_data',
        python_callable=load_task
    )
    
    extract_data_db_clean = PythonOperator(
        task_id='extract_clean_data',
        python_callable=extract_clean_task
    )

    check_model = PythonOperator(
        task_id='check_model',
        python_callable=check_model_task
)

    create_model = PythonOperator(
        task_id='create_model',
        python_callable=create_model_task
)
    load_model = PythonOperator(
        task_id='load_model',
        python_callable=load_model_data_task
    )

    dummy_end_task = EmptyOperator(
        task_id='dummy_end',
        trigger_rule='one_success'
    )

check_connection >> \
extract_data_kaggle_csv >> \
transform_and_load_csv >> \
extrac_data_db_raw >> \
transform_data_db_raw >> \
load_data_db_raw >> \
extract_data_db_clean >> \
check_model >> \
create_model >> \
load_model >> \
dummy_end_task

