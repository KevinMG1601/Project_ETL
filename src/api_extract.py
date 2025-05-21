import pandas as pd
import requests
import time

INPUT_CSV = '/home/valentina/github/Project_ETL/data/vias.csv'
OUTPUT_CSV = '/home/valentina/github/Project_ETL/data/vias_extracted.csv'
RADIUS_METERS = 51
TOP_FEATURES = ['highway', 'surface', 'lit', 'oneway', 'maxspeed']

# Función para consultar Overpass API
def fetch_from_overpass(lat, lon, radius=RADIUS_METERS):
    query = f"""
    [out:json][timeout:25];
    way(around:{radius},{lat},{lon});
    out tags;
    """
    url = "https://overpass-api.de/api/interpreter"
    try:
        response = requests.post(url, data={'data': query})
        if response.status_code == 200:
            data = response.json()
            if data.get('elements'):
                return data['elements'][0].get('tags', {})
    except Exception as e:
        print(f"error al consultar {lat}, {lon}: {e}")
    return {}
vias = pd.read_csv(INPUT_CSV)
records = []
#iterar sobre vías y consultar la API
for _, row in vias.iterrows():
    tags = fetch_from_overpass(row['latitude'], row['longitude'])
    record = {"road_name": row['road_name']}
    for feature in TOP_FEATURES:
        record[feature] = tags.get(feature)
    records.append(record)
    time.sleep(1)
df_result = pd.DataFrame(records)
df_result.to_csv(OUTPUT_CSV, index=False)
print(f"guardado: {OUTPUT_CSV}")