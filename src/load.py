import pandas as pd
import sys
import os
import numpy as np
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sqlalchemy import create_engine, text
from utils.connection_db import get_mysql_connection
from utils.db_initializer import create_db_and_user

def load_from_csv_to_model(path: str):
    create_db_and_user()  # Crea base y usuario si no existen
    engine = get_mysql_connection()
    df = load_merged_csv(path)
    create_dimensional_model_tables(engine)
    load_fact_table(df, engine)

def load_merged_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    def reverse_one_hot(df, prefix):
        cols = [col for col in df.columns if col.startswith(prefix)]
        df[prefix[:-1]] = df[cols].idxmax(axis=1).str.replace(f"{prefix}", "", regex=False)
        return df.drop(columns=cols)

    df = reverse_one_hot(df, "weather_condition_")
    df = reverse_one_hot(df, "traffic_control_device_")
    df = reverse_one_hot(df, "first_crash_type_")
    df = reverse_one_hot(df, "prim_contributory_cause_")

    df["injury_severity_encoded"] = df["injury_severity_encoded"].astype(str)
    df["highway"] = df["highway"].astype(str)  # <-- Asegura que highway exista para el merge

    return df

def create_dimensional_model_tables(engine):
    with engine.begin() as conn:
        # Eliminar tablas si existen (por orden de dependencias)
        conn.execute(text("DROP TABLE IF EXISTS fact_accidents"))
        conn.execute(text("DROP TABLE IF EXISTS dim_time"))
        conn.execute(text("DROP TABLE IF EXISTS dim_road"))
        conn.execute(text("DROP TABLE IF EXISTS dim_prim_contributory_cause"))
        conn.execute(text("DROP TABLE IF EXISTS dim_first_crash_type"))
        conn.execute(text("DROP TABLE IF EXISTS dim_traffic_control_device"))
        conn.execute(text("DROP TABLE IF EXISTS dim_weather_condition"))
        conn.execute(text("DROP TABLE IF EXISTS dim_injury_severity"))

        conn.execute(text("""
            CREATE TABLE dim_injury_severity (
                id INT AUTO_INCREMENT PRIMARY KEY,
                severity_label VARCHAR(100) UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE dim_weather_condition (
                id INT AUTO_INCREMENT PRIMARY KEY,
                `condition` VARCHAR(100) UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE dim_traffic_control_device (
                id INT AUTO_INCREMENT PRIMARY KEY,
                device_name VARCHAR(100) UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE dim_first_crash_type (
                id INT AUTO_INCREMENT PRIMARY KEY,
                crash_type VARCHAR(100) UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE dim_prim_contributory_cause (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cause_description VARCHAR(255) UNIQUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE dim_road (
                id INT AUTO_INCREMENT PRIMARY KEY,
                road_name VARCHAR(255),
                highway_type VARCHAR(50),
                surface VARCHAR(50),
                maxspeed_kmh FLOAT,
                lit BOOLEAN,
                oneway BOOLEAN,
                UNIQUE(road_name, highway_type, surface, maxspeed_kmh, lit, oneway)
            );
        """))
        conn.execute(text("""
            CREATE TABLE dim_time (
                id INT AUTO_INCREMENT PRIMARY KEY,
                year INT,
                month INT,
                day INT,
                hour INT,
                day_of_week INT,
                UNIQUE(year, month, day, hour, day_of_week)
            );
        """))
        conn.execute(text("""
            CREATE TABLE fact_accidents (
                id_fact INT AUTO_INCREMENT PRIMARY KEY,
                crash_date DATETIME,
                num_units INT,
                injury_severity_id INT,
                weather_id INT,
                traffic_control_id INT,
                crash_type_id INT,
                cause_id INT,
                road_id INT,
                time_id INT,
                FOREIGN KEY (injury_severity_id) REFERENCES dim_injury_severity(id),
                FOREIGN KEY (weather_id) REFERENCES dim_weather_condition(id),
                FOREIGN KEY (traffic_control_id) REFERENCES dim_traffic_control_device(id),
                FOREIGN KEY (crash_type_id) REFERENCES dim_first_crash_type(id),
                FOREIGN KEY (cause_id) REFERENCES dim_prim_contributory_cause(id),
                FOREIGN KEY (road_id) REFERENCES dim_road(id),
                FOREIGN KEY (time_id) REFERENCES dim_time(id)
            );
        """))

def insert_dimension_and_get_id(df_value, column_name, table_name, engine):
    with engine.begin() as conn:
        conn.execute(
            text(f"INSERT IGNORE INTO {table_name} (`{column_name}`) VALUES (:val)"),
            [{"val": str(v)} for v in df_value.dropna().unique()]
        )
    df_result = pd.read_sql(f"SELECT id, `{column_name}` FROM {table_name}", engine)
    df_result[column_name] = df_result[column_name].astype(str)
    return df_result

def insert_dim_road(df, engine):
    df_unique = df[["road_name", "highway", "surface", "maxspeed_kmh", "lit_bin", "oneway_bin"]].drop_duplicates()
    df_unique = df_unique.replace({np.nan: None})
    with engine.begin() as conn:
        for _, row in df_unique.iterrows():
            conn.execute(text("""
                INSERT IGNORE INTO dim_road (road_name, highway_type, surface, maxspeed_kmh, lit, oneway)
                VALUES (:road_name, :highway, :surface, :maxspeed, :lit, :oneway)
            """), {
                "road_name": row["road_name"],
                "highway": row["highway"],
                "surface": row["surface"],
                "maxspeed": row["maxspeed_kmh"],
                "lit": row["lit_bin"],
                "oneway": row["oneway_bin"]
            })
    return pd.read_sql("SELECT * FROM dim_road", engine)

def insert_dim_time(df, engine):
    df_time = df[["crash_year", "crash_month_num", "crash_day", "crash_hour", "crash_day_of_week"]].drop_duplicates()
    df_time.columns = ["year", "month", "day", "hour", "dow"]
    df_time = df_time.replace({np.nan: None})
    df_time = df_time[df_time[["year", "month", "day", "hour", "dow"]].notnull().all(axis=1)]
    with engine.begin() as conn:
        for _, row in df_time.iterrows():
            conn.execute(text("""
                INSERT IGNORE INTO dim_time (year, month, day, hour, day_of_week)
                VALUES (:year, :month, :day, :hour, :dow)
            """), {
                "year": int(row["year"]),
                "month": int(row["month"]),
                "day": int(row["day"]),
                "hour": int(row["hour"]),
                "dow": int(row["dow"])
            })
    return pd.read_sql("SELECT * FROM dim_time", engine)

def load_fact_table(df, engine):
    dims = {
        "injury_severity": insert_dimension_and_get_id(df["injury_severity_encoded"], "severity_label", "dim_injury_severity", engine),
        "weather": insert_dimension_and_get_id(df["weather_condition"], "condition", "dim_weather_condition", engine),
        "traffic": insert_dimension_and_get_id(df["traffic_control_device"], "device_name", "dim_traffic_control_device", engine),
        "crash_type": insert_dimension_and_get_id(df["first_crash_type"], "crash_type", "dim_first_crash_type", engine),
        "cause": insert_dimension_and_get_id(df["prim_contributory_cause"], "cause_description", "dim_prim_contributory_cause", engine),
        "road": insert_dim_road(df, engine),
        "time": insert_dim_time(df, engine)
    }

    df_fact = df.merge(dims["injury_severity"], left_on="injury_severity_encoded", right_on="severity_label") \
                .merge(dims["weather"], left_on="weather_condition", right_on="condition") \
                .merge(dims["traffic"], left_on="traffic_control_device", right_on="device_name") \
                .merge(dims["crash_type"], left_on="first_crash_type", right_on="crash_type") \
                .merge(dims["cause"], left_on="prim_contributory_cause", right_on="cause_description") \
                .merge(dims["road"], on=["road_name", "highway", "surface", "maxspeed_kmh", "lit_bin", "oneway_bin"]) \
                .merge(dims["time"], left_on=["crash_year", "crash_month_num", "crash_day", "crash_hour", "crash_day_of_week"],
                       right_on=["year", "month", "day", "hour", "day_of_week"])

    df_fact["crash_date"] = pd.to_datetime(df_fact[["crash_year", "crash_month_num", "crash_day", "crash_hour"]]
                                           .astype(str).agg(" ".join, axis=1), errors="coerce", format="%Y %m %d %H")

    to_insert = df_fact[[
        "crash_date", "num_units",
        "id_x", "id_y", "id_x.1", "id_y.1", "id_x.2", "id_y.2"
    ]]
    to_insert.columns = [
        "crash_date", "num_units",
        "injury_severity_id", "weather_id", "traffic_control_id",
        "crash_type_id", "cause_id", "road_id", "time_id"
    ]

    to_insert = to_insert.dropna(subset=["crash_date"])
    to_insert.to_sql("fact_accidents", con=engine, if_exists="append", index=False)

if __name__ == "__main__":
    load_from_csv_to_model("/home/valentina/github/Project_ETL/data/merged.csv")
    print("Carga completada.")
