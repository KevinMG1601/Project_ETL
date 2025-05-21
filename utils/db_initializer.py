from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def create_db_and_user():
    admin_user = os.getenv("MYSQL_ADMIN_USER", "root")
    admin_password = os.getenv("MYSQL_ADMIN_PASSWORD", "root")
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")

    db_name = os.getenv("MYSQL_DB", "accidents")
    app_user = os.getenv("MYSQL_USER", "simon")
    app_password = os.getenv("MYSQL_PASSWORD", "simon")

    # Conexión como root
    url = f"mysql+pymysql://{admin_user}:{admin_password}@{host}:{port}"
    engine = create_engine(url)

    with engine.begin() as conn:
        # Crear base de datos si no existe
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name};"))

        # Crear usuario si no existe
        conn.execute(text(
            f"CREATE USER IF NOT EXISTS '{app_user}'@'%' IDENTIFIED BY '{app_password}';"
        ))

        # Dar permisos al usuario
        conn.execute(text(
            f"GRANT ALL PRIVILEGES ON {db_name}.* TO '{app_user}'@'%';"
        ))

        conn.execute(text("FLUSH PRIVILEGES;"))
