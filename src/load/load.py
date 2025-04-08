import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

def load_data(df: pd.DataFrame):
    """
    Carga el DataFrame transformado a la base de datos db_clean
    en la tabla 'accidents_clean'.
    """
    print("📦 Cargando datos a accidents_clean...")

    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    DATABASE_CLEAN = os.getenv("DB_CLEAN")

    engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE_CLEAN}")

    try:
        df.to_sql(
            name="accidents_clean",
            con=engine,
            if_exists="replace",
            index=False
        )
        print("Datos limpios guardados en la tabla 'accidents_clean'")
    except Exception as e:
        print(f"Error cargando datos: {e}")
