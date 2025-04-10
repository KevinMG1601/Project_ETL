from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def extract_csv_to_parquet():
    csv_path = '/opt/airflow/datasets/df_limpio.csv'
    parquet_path = '/opt/airflow/datasets/df_limpio.parquet'

    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path, index=False)
    print(f"Archivo Parquet creado en: {parquet_path}")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='extract_csv_to_parquet',
    schedule_interval=None,  # Solo se ejecuta manualmente
    default_args=default_args,
    description='Convierte df_limpio.csv a df_limpio.parquet',
    tags=['etl', 'extract']
) as dag:

    extract_task = PythonOperator(
        task_id='convert_csv_to_parquet',
        python_callable=extract_csv_to_parquet
    )

    extract_task

