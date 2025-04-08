import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

def create_engine_connection(db_key="DB_RAW"):
    """
    Crea una conexión SQLAlchemy a la base de datos especificada usando variables de entorno.

    Parámetros:
    - db: Nombre de la variable de entorno que contiene el nombre de la base de datos.
            Por defecto se conecta a DB_RAW.

    Retorna:
    - engine de SQLAlchemy conectado a la base especificada.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv(db_key)

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(url)
    return engine
