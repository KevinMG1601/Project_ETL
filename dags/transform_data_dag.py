from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transform_data():
    # Ruta de entrada: archivo extraído en formato Parquet en la carpeta datasets
    input_path = '/opt/airflow/datasets/df.parquet'
    # Ruta de salida: archivo transformado en la misma carpeta
    output_path = '/opt/airflow/datasets/df_transformado.parquet'
    
    # Cargar el dataset extraído
    df = pd.read_parquet(input_path)

    # =================== LIMPIEZA BÁSICA ===================
    # Convertir columnas numéricas y rellenar nulos (si corresponde)
    df['injuries_total'] = pd.to_numeric(df['injuries_total'], errors='coerce').fillna(0).astype(int)
    df['injuries_fatal'] = pd.to_numeric(df['injuries_fatal'], errors='coerce').fillna(0).astype(int)
    df['injuries_incapacitating'] = pd.to_numeric(df['injuries_incapacitating'], errors='coerce').fillna(0).astype(int)
    df['injuries_non_incapacitating'] = pd.to_numeric(df['injuries_non_incapacitating'], errors='coerce').fillna(0).astype(int)
    df['injuries_reported_not_evident'] = pd.to_numeric(df['injuries_reported_not_evident'], errors='coerce').fillna(0).astype(int)
    df['injuries_no_indication'] = pd.to_numeric(df['injuries_no_indication'], errors='coerce').fillna(0).astype(int)
    
    # =================== LIMPIEZA EXTRA ===================
    # Normalización de cadenas: quitar espacios y pasar a mayúsculas para uniformidad
    cols_to_clean = [
        'traffic_control_device', 'weather_condition', 'lighting_condition',
        'first_crash_type', 'trafficway_type', 'roadway_surface_cond',
        'crash_type', 'prim_contributory_cause', 'most_severe_injury'
    ]
    for col in cols_to_clean:
        df[col] = df[col].astype(str).str.strip().str.upper()
    # Rellenar nulos con "UNKNOWN"
    df[cols_to_clean] = df[cols_to_clean].fillna('UNKNOWN')

    # =================== NUEVAS TRANSFORMACIONES ===================
    # 1. ¿El choque ocurrió de noche? (entre las 20:00 y 06:00)
    df['is_night'] = df['crash_hour'].apply(lambda x: x >= 20 or x < 6)

    # 2. Mapeo de severidad de lesiones a niveles numéricos
    severity_mapping = {
        'NO INDICATION OF INJURY': 0,
        'REPORTED, NOT EVIDENT': 1,
        'NONINCAPACITATING INJURY': 2,
        'INCAPACITATING INJURY': 3,
        'FATAL': 4,
        'UNKNOWN': -1
    }
    df['injury_severity_level'] = df['most_severe_injury'].map(severity_mapping).fillna(-1).astype(int)

    # 3. Agrupación de total de lesiones en rangos
    def group_injuries(total):
        if total == 0:
            return '0'
        elif total <= 2:
            return '1-2'
        elif total <= 5:
            return '3-5'
        else:
            return '>5'
    df['injuries_total_grouped'] = df['injuries_total'].apply(group_injuries)

    # 4. Indicador: ¿Hubo alguna lesión?
    df['has_injury'] = df['injuries_total'] > 0

    # 5. Ratio de lesiones: lesiones totales / (lesiones totales + lesiones no indicadas)
    df['injury_ratio'] = df.apply(
        lambda row: row['injuries_total'] / (row['injuries_total'] + row['injuries_no_indication'])
        if (row['injuries_total'] + row['injuries_no_indication']) > 0 else 0,
        axis=1
    )

    # 6. Clasificación general del tipo de choque
    def classify_crash_type(crash):
        if 'REAR END' in crash:
            return 'REAR_END'
        elif 'TURNING' in crash or 'ANGLE' in crash:
            return 'TURNING_OR_ANGLE'
        elif 'SIDESWIPE' in crash:
            return 'SIDESWIPE'
        elif 'PEDESTRIAN' in crash or 'PEDALCYCLIST' in crash:
            return 'VULNERABLE_USER'
        else:
            return 'OTHER'
    df['crash_class'] = df['first_crash_type'].apply(classify_crash_type)

    # 7. Construcción de una columna de datetime (si la información es suficiente)
    try:
        df['crash_datetime'] = pd.to_datetime({
            'year': df['crash_year'],
            'month': df['crash_month'],
            'day': 1,       # Se asume el día 1 como placeholder
            'hour': df['crash_hour']
        })
    except Exception as e:
        print(f"Error creando columna 'crash_datetime': {e}")

    # =================== GUARDADO ===================
    # Guardamos el DataFrame transformado
    df.to_parquet(output_path, index=False)
    print(f"Datos transformados guardados en: {output_path}")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

with DAG(
    dag_id='transform_data_dag',
    default_args=default_args,
    schedule_interval=None,  # Puedes ajustar el intervalo si deseas automatizarlo (ej: '@daily')
    description='DAG para transformar el dataset de accidentes',
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    transform_task

