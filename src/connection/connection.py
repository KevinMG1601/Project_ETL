import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

def create_engine_connection(db_key="DB_RAW"):
    """
    Crea una conexión SQLAlchemy a la base de datos especificada utilizando variables de entorno.

    Parámetros:
        db_key (str): Clave de la variable de entorno que contiene el nombre de la base de datos.
                      Por defecto es 'DB_RAW'.

    Retorna:
        sqlalchemy.engine.Engine: Objeto de conexión SQLAlchemy a la base de datos.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv(db_key)

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(url)
    return engine



