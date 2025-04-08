from src.connection.connection import create_engine_connection
import pandas as pd

def extract_data():
    engine = create_engine_connection(db_key="DB_RAW")

    raw_conn = engine.raw_connection()
    try:
        df = pd.read_sql("SELECT * FROM accidents", con=raw_conn)
    finally:
        raw_conn.close()

    return df
