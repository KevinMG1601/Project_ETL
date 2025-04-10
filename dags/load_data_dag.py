import pandas as pd
from sqlalchemy import create_engine

# Conexión a PostgreSQL
engine = create_engine('postgresql+psycopg2://usuario:contraseña@localhost:5432/accidentes_db')

# Diccionario con nombres de las tablas y rutas de archivos Parquet
tablas_dimensionales = {
    'fact_table': 'data/fact_table.parquet',
    'dim_tiempo': 'data/dim_tiempo.parquet',
    'dim_clima': 'data/dim_clima.parquet',
    'dim_localizacion': 'data/dim_localizacion.parquet',
    'dim_causa': 'data/dim_causa.parquet',
    'dim_lesion': 'data/dim_lesion.parquet',
    'dim_trafico': 'data/dim_trafico.parquet',
}

def load_dimensional_to_postgres():
    for tabla, path in tablas_dimensionales.items():
        print(f"Cargando {tabla} desde {path}...")
        df = pd.read_parquet(path)
        df.to_sql(tabla, engine, if_exists='replace', index=False)
        print(f"✅ Tabla '{tabla}' cargada con éxito.")

if __name__ == '__main__':
    load_dimensional_to_postgres()

