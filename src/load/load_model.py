import pandas as pd
from sqlalchemy import text
from src.connection.connection import create_engine_connection
from airflow.exceptions import AirflowFailException
from airflow.utils.log.logging_mixin import LoggingMixin



def check_model_exists(db_key: str = "DB_MODEL"):
    """
    Verifica si las tablas del modelo dimensional existen en la base de datos.
    """
    engine = create_engine_connection(db_key)
    tables = ['dim_lesion', 'dim_causa', 'dim_trafico', 'dim_clima', 'dim_tiempo', 'hechos_accidentes']
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = :table;
    """
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(query), {"table": table}).fetchone()
            if not result:
                print(f"Tabla {table} no existe.")
                return False
    print("Todas las tablas del modelo existen.")
    return True


def create_model_tables():
    try:
        engine = create_engine_connection("DB_MODEL")  

        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS dim_lesion (
                id_lesion INT AUTO_INCREMENT PRIMARY KEY,
                most_severe_injury TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_causa (
                id_causa INT AUTO_INCREMENT PRIMARY KEY,
                prim_contributory_cause TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_trafico (
                id_trafico INT AUTO_INCREMENT PRIMARY KEY,
                intersection_related_i TEXT,
                traffic_control_device TEXT,
                trafficway_type TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_clima (
                id_clima INT AUTO_INCREMENT PRIMARY KEY,
                lighting_condition TEXT,
                roadway_surface_cond TEXT,
                weather_condition TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_tiempo (
                id_tiempo INT AUTO_INCREMENT PRIMARY KEY,
                crash_day_of_week TEXT,
                crash_hour INT,
                crash_month INT,
                crash_year INT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS hechos_accidentes (
                id INT AUTO_INCREMENT PRIMARY KEY,
                crash_type TEXT,
                first_crash_type TEXT,
                id_causa INT,
                id_clima INT,
                id_lesion INT,
                id_tiempo INT,
                id_trafico INT,
                FOREIGN KEY (id_causa) REFERENCES dim_causa(id_causa),
                FOREIGN KEY (id_clima) REFERENCES dim_clima(id_clima),
                FOREIGN KEY (id_lesion) REFERENCES dim_lesion(id_lesion),
                FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo),
                FOREIGN KEY (id_trafico) REFERENCES dim_trafico(id_trafico)
            )
            """
        ]

        with engine.connect() as conn:
            with conn.begin():
                for ddl in ddl_statements:
                    conn.execute(text(ddl))
        print("Tablas del modelo dimensional creadas con éxito.")

    except Exception as e:
        print("Error al crear las tablas del modelo dimensional:", e)
        raise