import pandas as pd
input_file = '/home/valentina/github/Project_ETL/data/traffic_accidents.csv'
output_file = '/home/valentina/github/Project_ETL/data/dataset_extracted.csv'
selected_columns = [
    'crash_date',
    'crash_hour',
    'crash_day_of_week',
    'weather_condition',
    'traffic_control_device',
    'first_crash_type',
    'prim_contributory_cause',
    'num_units',
    'most_severe_injury'
]
df = pd.read_csv(input_file)
missing_cols = [col for col in selected_columns if col not in df.columns]
if missing_cols:
    raise ValueError(f"Las siguientes columnas no están en el archivo: {missing_cols}")
df_selected = df[selected_columns]
df_selected.to_csv(output_file, index=False)
print(f"Archivo guardado exitosamente en: {output_file}")
