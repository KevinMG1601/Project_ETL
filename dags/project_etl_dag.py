from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Ruta al archivo CSV original (asegúrate que esté disponible en el contenedor)
CSV_PATH = "/opt/airflow/datasets/df_limpio.csv"
PARQUET_PATH = "/opt/airflow/datasets/df_limpio.parquet"

def extract_csv_to_parquet():
    df = pd.read_csv(CSV_PATH)
    df.to_parquet(PARQUET_PATH, index=False)
    print(f"Archivo Parquet guardado en {PARQUET_PATH}")

with DAG(
    dag_id="project_etl_pipeline",
    start_date=datetime(2025, 4, 9),
    schedule_interval=None,
    catchup=False,
    tags=["etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_csv_to_parquet",
        python_callable=extract_csv_to_parquet
    )

    extract_task

