import pandas as pd
from src.load.load import load_data
from datetime import datetime

def transform_csv(csv_path: str, db_key: str = "DB_RAW"):
    """
    Transforma los datos del CSV y los carga en la base de datos DB_RAW.
    Si es necesario, cambia el formato de las fechas antes de cargar los datos en la base de datos.

    Parámetros:
    - csv_path (str): Ruta al archivo CSV que contiene los datos.
    - db_key (str): Clave de la base de datos a utilizar (default: "DB_RAW").
    """
    df = pd.read_csv(csv_path)

    if 'crash_date' in df.columns:  

        df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')

    load_data(df, db_key)