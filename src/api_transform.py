import pandas as pd
import re

INPUT_FILE = '/home/valentina/github/Project_ETL/data/vias_extracted.csv'
OUTPUT_FILE = '/home/valentina/github/Project_ETL/data/vias_transformed.csv'

def convert_maxspeed(value):
    if pd.isna(value):
        return None
    match = re.search(r'\d+', str(value))
    if not match:
        return None
    speed = int(match.group())
    if 'mph' in str(value).lower():
        return round(speed * 1.609, 1)  # Convertir mph a km/h
    return speed

def transform_vias():
    df = pd.read_csv(INPUT_FILE)

    # estandarizar maxspeed
    df['maxspeed_kmh'] = df['maxspeed'].apply(convert_maxspeed)

    # codificación binaria
    df['lit_bin'] = df['lit'].apply(lambda x: 1 if x == 'yes' else 0)
    df['oneway_bin'] = df['oneway'].apply(lambda x: 1 if x == 'yes' else (0 if x == 'no' else None))

    # one hot encoding
    df_encoded = pd.get_dummies(df[['highway', 'surface']], prefix=['highway', 'surface'])

    # ensamblar DataFrame final
    final_df = pd.concat([
        df[['road_name', 'highway', 'surface', 'maxspeed_kmh', 'lit_bin', 'oneway_bin']],
        df_encoded
    ], axis=1)

    # eliminar columnas donde todos los valores sean 0
    final_df = final_df.loc[:, (final_df != 0).any(axis=0)]

    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"transformación completada, guardado: {OUTPUT_FILE}")

if __name__ == "__main__":
    transform_vias()