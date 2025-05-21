import pandas as pd
accidentes_df = pd.read_csv("/home/valentina/github/Project_ETL/data/dataset_transformed.csv")
vias_df = pd.read_csv("/home/valentina/github/Project_ETL/data/vias_transformed.csv")
merged_df = accidentes_df.merge(vias_df, how='left', on='road_name')
print("registros antes del merge", len(accidentes_df))
print("registros despues del merge", len(merged_df))
print("columnas nuevas agregadas", set(merged_df.columns) - set(accidentes_df.columns))
merged_df.to_csv("/home/valentina/github/Project_ETL/data/merged.csv", index=False)
print("merged guardado")