import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Configuraciones
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette('Set2')
OUTPUT_DIR = 'eda_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Carga de datos
df = pd.read_csv('../data/df_limpio.csv')

print(f"\n🔹 Dimensiones: {df.shape}\n")

print("🔹 Tipos de datos:")
print(df.dtypes, "\n")

print("🔹 Primeras filas:")
print(df.head(), "\n")

print("🔹 Valores nulos por columna:")
print(df.isnull().sum(), "\n")

print("🔹 Estadísticas descriptivas:")
print(df.describe(include='all'), "\n")

# Columnas categóricas
cat_cols = df.select_dtypes(include='object').columns.tolist()
print("🔹 Columnas categóricas:", cat_cols, "\n")

# Frecuencia de cada variable categórica
for col in cat_cols:
    print(f"📊 Frecuencia de '{col}':")
    print(df[col].value_counts().head(10), "\n")

# Distribución de hora del accidente
plt.figure(figsize=(10, 6))
sns.countplot(data=df, x='crash_hour')
plt.title('Distribución de accidentes por hora del día')
plt.xticks(rotation=45)
plt.xlabel('Hora')
plt.ylabel('Número de accidentes')
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/accidentes_por_hora.png')
plt.close()

# Accidentes por día de la semana
plt.figure(figsize=(10, 6))
dias = ['Lun', 'Mar', 'Mié', 'Jue', 'Vie', 'Sáb', 'Dom']
sns.countplot(data=df, x='crash_day_of_week')
plt.title('Accidentes por día de la semana')
plt.xticks(ticks=range(7), labels=dias)
plt.xlabel('Día de la semana')
plt.ylabel('Número de accidentes')
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/accidentes_por_dia.png')
plt.close()

# Accidentes por condición climática
plt.figure(figsize=(10, 6))
top_weather = df['weather_condition'].value_counts().head(10)
sns.barplot(x=top_weather.index, y=top_weather.values)
plt.title('Top 10 condiciones climáticas en accidentes')
plt.xticks(rotation=45)
plt.xlabel('Condición climática')
plt.ylabel('Número de accidentes')
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/accidentes_por_clima.png')
plt.close()

# Mapa de calor de correlación
plt.figure(figsize=(12, 8))
sns.heatmap(df.select_dtypes(include='number').corr(), annot=True, cmap='coolwarm')
plt.title('Correlación entre variables numéricas')
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/heatmap_correlacion.png')
plt.close()

# Lesiones según tipo de colisión
plt.figure(figsize=(12, 6))
top_crash = df['first_crash_type'].value_counts().head(7).index
sub_df = df[df['first_crash_type'].isin(top_crash)]
sns.boxplot(data=sub_df, x='first_crash_type', y='injuries_total')
plt.title('Lesiones por tipo de primera colisión')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/lesiones_por_tipo_colision.png')
plt.close()

print(f"✅ EDA completado. Gráficos guardados en la carpeta: {OUTPUT_DIR}/")

