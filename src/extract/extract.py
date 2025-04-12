import os
import pandas as pd
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import datetime
from src.connection.connection import create_engine_connection

def extract_data(db_key: str) -> pd.DataFrame:
    """
    Extrae los datos de la tabla 'accidents' desde la base de datos RAW.

    Retorna:
        pd.DataFrame: DataFrame con los datos crudos extraídos desde la base de datos.
    """
    engine = create_engine_connection(db_key)

    raw_conn = engine.raw_connection()
    if db_key == "DB_RAW":
        table_name = "raw_accidents"
    elif db_key == "DB_CLEAN":
        table_name = "clean_accidents"
    else: 
        db_key = "DB_MODEL"
        table_name = "model_accidents"

    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=raw_conn)
    finally:
        raw_conn.close()

    return df



def extract_data_kaggle(dataset_id, download_dir):
    """
    Descarga el dataset desde Kaggle y lo guarda en un directorio local.
    Retorna True si la descarga fue exitosa, de lo contrario False.
    """
    api = KaggleApi()
    api.authenticate()

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    print(f"Descargando el dataset {dataset_id} en {download_dir}...")

    try:
 
        api.dataset_download_files(dataset_id, path=download_dir, unzip=True)
        print(f"Dataset descargado exitosamente en {download_dir}")

       
        if os.listdir(download_dir):  
            return True
        else:
            print(f"El directorio {download_dir} está vacío después de la extracción.")
            return False
    except Exception as e:
        print(f"Error al descargar el dataset {dataset_id}: {str(e)}")
        return False



