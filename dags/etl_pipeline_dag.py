import os
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ----------------- FUNCIONES ETL -----------------

def extract_csv_to_parquet():
    input_path = '/opt/airflow/datasets/df_limpio.csv'
    output_path = '/opt/airflow/datasets/df_limpio.parquet'
    
    df = pd.read_csv(input_path)
    df.to_parquet(output_path, index=False)
    print(f"Archivo convertido a Parquet: {output_path}")

def transform_data():
    input_path = '/opt/airflow/datasets/df_limpio.parquet'
    output_path = '/opt/airflow/datasets/df_transformado.parquet'
    
    df = pd.read_parquet(input_path)

    # Ejemplo de transformación: eliminar duplicados, convertir tipos
    df.drop_duplicates(inplace=True)
    df['crash_month'] = df['crash_month'].astype(int)
    
    df.to_parquet(output_path, index=False)
    print(f"Datos transformados guardados en: {output_path}")

def create_dimensional_model():
    input_path = '/opt/airflow/datasets/df_transformado.parquet'
    output_dir = '/opt/airflow/dimensional'
    os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_parquet(input_path)

    # Tabla de dimensión tiempo
    dim_tiempo = df[['crash_year', 'crash_month', 'crash_day_of_week', 'crash_hour']].drop_duplicates().copy()
    dim_tiempo['id_tiempo'] = range(1, len(dim_tiempo) + 1)

    # Tabla de dimensión clima
    dim_clima = df[['weather_condition', 'lighting_condition', 'roadway_surface_cond']].drop_duplicates().copy()
    dim_clima['id_clima'] = range(1, len(dim_clima) + 1)

    # Tabla de dimensión tipo de tráfico
    dim_trafico = df[['traffic_control_device', 'trafficway_type', 'intersection_related_i']].drop_duplicates().copy()
    dim_trafico['id_trafico'] = range(1, len(dim_trafico) + 1)

    # Tabla de dimensión causa
    dim_causa = df[['prim_contributory_cause']].drop_duplicates().copy()
    dim_causa['id_causa'] = range(1, len(dim_causa) + 1)

    # Tabla de dimensión tipo de lesión
    dim_lesion = df[['most_severe_injury']].drop_duplicates().copy()
    dim_lesion['id_lesion'] = range(1, len(dim_lesion) + 1)

    # Tabla de hechos
    fact = df.merge(dim_tiempo, on=['crash_year', 'crash_month', 'crash_day_of_week', 'crash_hour']) \
             .merge(dim_clima, on=['weather_condition', 'lighting_condition', 'roadway_surface_cond']) \
             .merge(dim_trafico, on=['traffic_control_device', 'trafficway_type', 'intersection_related_i']) \
             .merge(dim_causa, on='prim_contributory_cause') \
             .merge(dim_lesion, on='most_severe_injury')
    
    fact_table = fact[[
        'id_tiempo', 'id_clima', 'id_trafico', 'id_causa', 'id_lesion',
        'crash_type', 'first_crash_type'
    ]]

    # Guardar todo
    dim_tiempo.to_parquet(f"{output_dir}/dim_tiempo.parquet", index=False)
    dim_clima.to_parquet(f"{output_dir}/dim_clima.parquet", index=False)
    dim_trafico.to_parquet(f"{output_dir}/dim_trafico.parquet", index=False)
    dim_causa.to_parquet(f"{output_dir}/dim_causa.parquet", index=False)
    dim_lesion.to_parquet(f"{output_dir}/dim_lesion.parquet", index=False)
    fact_table.to_parquet(f"{output_dir}/hechos_accidentes.parquet", index=False)

    print("Modelo dimensional generado y guardado.")

def load_to_postgres():
    db_uri = 'postgresql://airflow:airflow@postgres:5432/airflow'
    engine = create_engine(db_uri)

    files = {
        'dim_tiempo': '/opt/airflow/dimensional/dim_tiempo.parquet',
        'dim_clima': '/opt/airflow/dimensional/dim_clima.parquet',
        'dim_trafico': '/opt/airflow/dimensional/dim_trafico.parquet',
        'dim_causa': '/opt/airflow/dimensional/dim_causa.parquet',
        'dim_lesion': '/opt/airflow/dimensional/dim_lesion.parquet',
        'hechos_accidentes': '/opt/airflow/dimensional/hechos_accidentes.parquet'
    }

    for table_name, file_path in files.items():
        df = pd.read_parquet(file_path)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Tabla cargada en PostgreSQL: {table_name}")

# ----------------- DAG CONFIG -----------------

default_args = {
    'owner': 'simón',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['etl'],
) as dag:

    extract = PythonOperator(
        task_id='extract_csv_to_parquet',
        python_callable=extract_csv_to_parquet
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    model = PythonOperator(
        task_id='create_dimensional_model',
        python_callable=create_dimensional_model
    )

    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    extract >> transform >> model >> load

