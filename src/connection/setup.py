from src.connection.connection import create_engine_connection
import sqlalchemy

def setup_dbs():
    """
    Verifica conexiones a las bases de datos RAW, CLEAN y MODEL.
    Crea las tablas raw_accidents y clean_accidents si no existen.
    """
    raw_engine = create_engine_connection("DB_RAW")
    clean_engine = create_engine_connection("DB_CLEAN")
    model_engine = create_engine_connection("DB_MODEL")

    for name, engine in {
        "DB_RAW": raw_engine, 
        "DB_CLEAN": clean_engine, 
        "DB_MODEL": model_engine
    }.items():
        try:
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
                print(f"Conectado a {name}")
        except Exception as e:
            raise RuntimeError(f"No se pudo conectar a {name}: {str(e)}")

    raw_sql = """
    CREATE TABLE IF NOT EXISTS raw_accidents (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data JSON
    )
    """
    clean_sql = """
    CREATE TABLE IF NOT EXISTS clean_accidents (
        id INT AUTO_INCREMENT PRIMARY KEY,
        data JSON
    )
    """

    with raw_engine.begin() as conn:
        conn.execute(sqlalchemy.text(raw_sql))
    with clean_engine.begin() as conn:
        conn.execute(sqlalchemy.text(clean_sql))
