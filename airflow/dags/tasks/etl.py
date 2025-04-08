import json
from src.extract.extract import extract_data
from src.transform.transform import transform_data
from src.load.load import load_data

def extract_task(**kwargs):
    df = extract_data()
    df_json = df.to_json(orient="records")
    kwargs['ti'].xcom_push(key='extracted_data', value=df_json)

def transform_task(**kwargs):
    df_json = kwargs['ti'].xcom_pull(key='extracted_data')
    df_transformed = transform_data(df_json)
    transformed_json = df_transformed.to_json(orient="records")
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_json)

def load_task(**kwargs):
    transformed_json = kwargs['ti'].xcom_pull(key='transformed_data')
    df_transformed = json.loads(transformed_json)
    load_data(df_transformed)

def validate_task(**kwargs):
    print("Validación pendiente de implementación.")
