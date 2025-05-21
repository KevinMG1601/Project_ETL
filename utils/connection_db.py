from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()
def get_mysql_connection():
    """
    Crea una conexión a la base de datos MySQL especificada en el archivo .env
    Retorna un SQLAlchemy Engine.
    """
    user = os.getenv("MYSQL_USER", "simon")
    password = os.getenv("MYSQL_PASSWORD", "simon")
    host = os.getenv("MYSQL_HOST", "%")
    port = os.getenv("MYSQL_PORT", "3306")
    database = os.getenv("MYSQL_DB", "accidents")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)
