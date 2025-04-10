import pandas as pd

# Ruta al archivo parquet (ajusta según dónde esté guardado tu archivo)
df = pd.read_csv("./df_limpio.csv")

# Mostrar las 20 primeras filas
print(df.head(20))

