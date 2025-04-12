import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

def load_data(df: pd.DataFrame, db_key: str):
    """
    Carga el DataFrame transformado a la base de datos db_clean
    en la tabla 'accidents_clean'.
    """
    print("Cargando datos a accidents_clean...")

    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    DB = os.getenv(db_key)

    engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}")

    if db_key == "DB_CLEAN":
        table_name = "clean_accidents"
    elif db_key == "DB_MODEL":
        table_name = "model_accidents"
    else:
        table_name = "raw_accidents"

    try:
        df.to_sql(
            name= table_name,
            con=engine,
            if_exists="replace",
            index=False
        )
        print("Datos limpios guardados en la tabla 'accidents_clean'")
    except Exception as e:
        print(f"Error cargando datos: {e}")
