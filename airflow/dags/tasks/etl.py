import json
import os
import pandas as pd
from datetime import datetime
from airflow.exceptions import AirflowFailException
from sqlalchemy import text
from airflow.models import Variable
from src.connection.setup import setup_dbs
from src.extract.extract import extract_data, extract_data_kaggle
from src.transform.transform_csv import transform_csv
from src.load.load import load_data
from src.transform.transform import transform_data
from src.load.load_model import check_model_exists, create_model_tables
from src.connection.connection import create_engine_connection

def check_conn_task(**kwargs):
    setup_dbs()


def extract_kaggle_task(**kwargs):
    """
    Verifica si los datos ya están cargados en DB_RAW.
    Si no están, descarga el dataset de Kaggle y lo guarda localmente.
    """
    
    db_key = "DB_RAW"
    table_name = "raw_accidents"
    dataset_id = 'oktayrdeki/traffic-accidents' 
    download_dir = '/home/kevin/github/Project_ETL/data'
    
    engine = create_engine_connection(db_key)
    conn = engine.raw_connection()

    try:
        df = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", con=conn)
        n_rows = df["count"].iloc[0]
        print(f"[INFO] La tabla '{table_name}' contiene {n_rows} filas.")
        
        if n_rows > 0:
            print("[INFO] Los datos ya están cargados en DB_RAW. No es necesario descargar de Kaggle.")
            return  
    except Exception as e:
        print(f"[WARNING] No se pudo consultar la tabla '{table_name}': {str(e)}")
        print("[INFO] Se intentará descargar los datos desde Kaggle.")
    finally:
        conn.close()

    extract_data_kaggle(dataset_id, download_dir)
    print("[INFO] Datos descargados correctamente desde Kaggle.")



def transform_csv_task(**kwargs):
    """
    Transforma los datos extraídos (EDA, limpieza, imputación) y los guarda en DB_RAW,
    solo si aún no están cargados en la base de datos.
    """

    db_key = "DB_RAW"
    table_name = "raw_accidents"
    csv_path = Variable.get("KAGGLE_DATA_PATH")  

    engine = create_engine_connection(db_key)
    conn = engine.raw_connection()
    
    try:
        df = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", con=conn)
        n_rows = df["count"].iloc[0]
        print(f"[INFO] La tabla '{table_name}' ya contiene {n_rows} filas.")
        
        if n_rows > 0:
            print("[INFO] Ya existen datos transformados en DB_RAW. Se omite esta tarea.")
            return  
    except Exception as e:
        print(f"[WARNING] No se pudo leer la tabla '{table_name}': {str(e)}")
        print("[INFO] Se procederá con la transformación del CSV.")
    finally:
        conn.close()

    transform_csv(csv_path, db_key)
    print("[INFO] Transformación y carga del CSV completadas.")




def extract_task(**kwargs):
    """
    Extrae los datos desde la base DB_RAW y los pasa como JSON por XCom.
    """
    df = extract_data("DB_RAW")
    df_json = df.to_json(orient="records")
    kwargs['ti'].xcom_push(key='extracted_data', value=df_json)


def transform_task(**kwargs):
    """
    Transforma los datos extraídos (EDA, limpieza, imputación) y los guarda como JSON en XCom.
    """
    df_json = kwargs['ti'].xcom_pull(key='extracted_data')
    df = pd.read_json(df_json)
    df_transformed = transform_data(df)
    transformed_json = df_transformed.to_json(orient="records")
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_json)


def load_task(**kwargs):
    """
    Carga los datos transformados en la base de datos DB_CLEAN.
    """
    transformed_json = kwargs['ti'].xcom_pull(key='transformed_data')
    df_transformed = pd.read_json(transformed_json)
    load_data(df_transformed, "DB_CLEAN")


def extract_clean_task(**kwargs):
    """
    Extrae los datos desde la base DB_RAW y los pasa como JSON por XCom.
    """
    df = extract_data("DB_CLEAN")
    df_json = df.to_json(orient="records")
    kwargs['ti'].xcom_push(key='extracted_data', value=df_json)


def check_model_task():
    return check_model_exists("DB_MODEL")

def create_model_task():
    return create_model_tables()


def load_model_data_task(**kwargs):
    """
    Carga los datos transformados a las tablas del modelo dimensional (dimensional model).
    Inserta datos en las tablas de dimensión y luego en la tabla de hechos.
    """
    engine_clean = create_engine_connection("DB_CLEAN")
    engine_model = create_engine_connection("DB_MODEL")

    try:
        query = "SELECT * FROM clean_accidents"
        df_clean = pd.read_sql(query, engine_clean)

        if df_clean.empty:
            raise AirflowFailException("No hay datos en la tabla clean_accidents para cargar al modelo.")
        
        print(f"Se han extraído {len(df_clean)} registros de la tabla clean_accidents.")

        lesion_data = df_clean[['most_severe_injury']].drop_duplicates()
        lesion_data.to_sql('dim_lesion', con=engine_model, if_exists='append', index=False)

        causa_data = df_clean[['prim_contributory_cause']].drop_duplicates()
        causa_data.to_sql('dim_causa', con=engine_model, if_exists='append', index=False)

        trafico_data = df_clean[['intersection_related_i', 'traffic_control_device', 'trafficway_type']].drop_duplicates()
        trafico_data.to_sql('dim_trafico', con=engine_model, if_exists='append', index=False)

        clima_data = df_clean[['lighting_condition', 'roadway_surface_cond', 'weather_condition']].drop_duplicates()
        clima_data.to_sql('dim_clima', con=engine_model, if_exists='append', index=False)

        tiempo_data = df_clean[['crash_day_of_week', 'crash_hour', 'crash_month', 'crash_year']].drop_duplicates()
        tiempo_data.to_sql('dim_tiempo', con=engine_model, if_exists='append', index=False)

        print("Datos cargados en las tablas de dimensiones exitosamente.")

        for _, row in df_clean.iterrows():
            insert_query = text("""
                INSERT INTO hechos_accidentes (
                    crash_type, first_crash_type, id_causa, id_clima, id_lesion, id_tiempo, id_trafico,
                    num_units, injuries_total, injuries_fatal, injuries_incapacitating, injuries_non_incapacitating,
                    injuries_reported_not_evident, injuries_no_indication, crash_day_of_month, damage_min, damage_max
                )
                SELECT 
                    :crash_type, :first_crash_type, 
                    c.id_causa, 
                    cl.id_clima, 
                    l.id_lesion, 
                    t.id_tiempo, 
                    tr.id_trafico,
                    :num_units, :injuries_total, :injuries_fatal, :injuries_incapacitating, :injuries_non_incapacitating,
                    :injuries_reported_not_evident, :injuries_no_indication, :crash_day_of_month, :damage_min, :damage_max
                FROM 
                    dim_causa c
                    JOIN dim_clima cl ON cl.lighting_condition = :lighting_condition
                    JOIN dim_lesion l ON l.most_severe_injury = :most_severe_injury
                    JOIN dim_tiempo t ON t.crash_day_of_week = :crash_day_of_week AND t.crash_hour = :crash_hour
                    JOIN dim_trafico tr ON tr.intersection_related_i = :intersection_related_i
                WHERE 
                    c.prim_contributory_cause = :prim_contributory_cause
                    AND cl.weather_condition = :weather_condition
            """)

            engine_model.execute(insert_query, {
                'crash_type': row['crash_type'],
                'first_crash_type': row['first_crash_type'],
                'prim_contributory_cause': row['prim_contributory_cause'],
                'lighting_condition': row['lighting_condition'],
                'most_severe_injury': row['most_severe_injury'],
                'crash_day_of_week': row['crash_day_of_week'],
                'crash_hour': row['crash_hour'],
                'intersection_related_i': row['intersection_related_i'],
                'num_units': row['num_units'],
                'injuries_total': row['injuries_total'],
                'injuries_fatal': row['injuries_fatal'],
                'injuries_incapacitating': row['injuries_incapacitating'],
                'injuries_non_incapacitating': row['injuries_non_incapacitating'],
                'injuries_reported_not_evident': row['injuries_reported_not_evident'],
                'injuries_no_indication': row['injuries_no_indication'],
                'crash_day_of_month': row['crash_day_of_month'],
                'damage_min': row['damage_min'],
                'damage_max': row['damage_max'],
                'weather_condition': row['weather_condition']
            })

        print("Datos cargados exitosamente en la tabla de hechos.")
        kwargs['ti'].xcom_push(key='load_to_model_status', value=True)

    except Exception as e:
        print(f"Error al cargar los datos al modelo dimensional: {str(e)}")
        kwargs['ti'].xcom_push(key='load_to_model_status', value=False)
        raise AirflowFailException(f"Error al cargar los datos al modelo dimensional: {str(e)}")

    return 'next_task'


