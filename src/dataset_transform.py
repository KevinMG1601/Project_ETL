import pandas as pd
import random
input_file = '/home/valentina/github/Project_ETL/data/dataset_extracted.csv'
output_file = '/home/valentina/github/Project_ETL/data/dataset_transformed.csv'
vias_file = '/home/valentina/github/Project_ETL/data/vias.csv'
df = pd.read_csv(input_file)
df_vias = pd.read_csv(vias_file)
vias = df_vias['road_name'].dropna().unique().tolist()
#asignar aleatoriamente una vía a cada fila
random.seed(42)
df['road_name'] = [random.choice(vias) for _ in range(len(df))]
#separar crash_date en año, mes y día
df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
df['crash_year'] = df['crash_date'].dt.year
df['crash_month_num'] = df['crash_date'].dt.month
df['crash_day'] = df['crash_date'].dt.day
df.drop(columns='crash_date', inplace=True)
#normalizar texto en columnas categóricas
text_columns = [
    'weather_condition',
    'traffic_control_device',
    'first_crash_type',
    'prim_contributory_cause',
    'most_severe_injury'
]
for col in text_columns:
    df[col] = df[col].astype(str).str.strip().str.upper()
#ohe para columnas categóricas
categorical_columns = [
    'weather_condition',
    'traffic_control_device',
    'first_crash_type',
    'prim_contributory_cause'
]
df = pd.get_dummies(df, columns=categorical_columns, drop_first=False)
#codificacion ordinal del target
injury_order = [
    'NO INDICATION OF INJURY',
    'REPORTED, NOT EVIDENT',
    'NONINCAPACITATING INJURY',
    'INCAPACITATING INJURY',
    'FATAL INJURY'
]
df['injury_severity_encoded'] = df['most_severe_injury'].apply(
    lambda x: injury_order.index(x) if x in injury_order else -1
)
df.drop(columns='most_severe_injury', inplace=True)
df = df.loc[:, (df != 0).any(axis=0)]
df.to_csv(output_file, index=False)
print(f"transformacion completada guardada {output_file}")
