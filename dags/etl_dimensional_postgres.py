from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Configuración conexión PostgreSQL
USER = 'airflow'
PASSWORD = 'airflow'
HOST = 'postgres'
PORT = '5432'
DB = 'accidents_db'
DATABASE_URL = f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}'

def etl_dimensional():
    engine = create_engine(DATABASE_URL)
    df = pd.read_parquet('/opt/airflow/dataset/accidentes_transformado.parquet')

    # Dimensiones simples
    def create_dimension(table_name, column_name):
        unique_vals = df[[column_name]].drop_duplicates().reset_index(drop=True)
        unique_vals[f"{table_name}_id"] = unique_vals.index + 1
        unique_vals.to_sql(table_name, engine, if_exists='replace', index=False)
        return unique_vals

    # Dimensión de tiempo
    dim_datetime = df[['crash_hour', 'crash_day_of_week', 'crash_month', 'crash_year']].drop_duplicates().reset_index(drop=True)
    dim_datetime['datetime_id'] = dim_datetime.index + 1
    dim_datetime.to_sql('dim_datetime', engine, if_exists='replace', index=False)

    # Otras dimensiones
    dim_weather = create_dimension('dim_weather', 'weather_condition')
    dim_lighting = create_dimension('dim_lighting', 'lighting_condition')
    dim_surface = create_dimension('dim_surface', 'roadway_surface_cond')
    dim_traffic_device = create_dimension('dim_traffic_control_device', 'traffic_control_device')
    dim_trafficway = create_dimension('dim_trafficway_type', 'trafficway_type')
    dim_crash_type = create_dimension('dim_crash_type', 'crash_type')
    dim_cause = create_dimension('dim_contributory_cause', 'prim_contributory_cause')
    dim_injury = create_dimension('dim_injury', 'most_severe_injury')

    # Merge de IDs
    df = df.merge(dim_datetime, on=['crash_hour', 'crash_day_of_week', 'crash_month', 'crash_year'], how='left')
    df = df.merge(dim_weather, on='weather_condition', how='left')
    df = df.merge(dim_lighting, on='lighting_condition', how='left')
    df = df.merge(dim_surface, on='roadway_surface_cond', how='left')
    df = df.merge(dim_traffic_device, on='traffic_control_device', how='left')
    df = df.merge(dim_trafficway, on='trafficway_type', how='left')
    df = df.merge(dim_crash_type, on='crash_type', how='left')
    df = df.merge(dim_cause, on='prim_contributory_cause', how='left')
    df = df.merge(dim_injury, on='most_severe_injury', how='left')

    # Tabla de hechos
    fact_df = df[[
        'datetime_id', 'dim_weather_id', 'dim_lighting_id', 'dim_surface_id',
        'dim_traffic_control_device_id', 'dim_trafficway_type_id', 'dim_crash_type_id',
        'intersection_related_i', 'dim_contributory_cause_id', 'dim_injury_id',
        'injuries_total', 'injuries_fatal', 'injuries_incapacitating',
        'injuries_non_incapacitating', 'injuries_reported_not_evident', 'injuries_no_indication'
    ]].copy()

    fact_df.rename(columns={'intersection_related_i': 'intersection_related'}, inplace=True)
    fact_df['accident_id'] = fact_df.index + 1
    fact_df = fact_df[['accident_id'] + [col for col in fact_df.columns if col != 'accident_id']]

    fact_df.to_sql('fact_accidents', engine, if_exists='replace', index=False)

# DAG de Airflow
with DAG(
    dag_id='etl_dimensional_postgres',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'postgres', 'dimensional'],
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_dimensional',
        python_callable=etl_dimensional
    )

    run_etl

