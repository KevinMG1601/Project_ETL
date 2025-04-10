🧱 Proceso ETL: Segunda Entrega
1. Extracción
Se parte de un archivo .csv limpio llamado df_limpio.csv, que contiene datos de accidentes de tráfico.

Este archivo se lee utilizando Pandas y se convierte a formato .parquet para optimizar el rendimiento en las fases siguientes del pipeline.

2. Transformación
Durante esta fase se aplicaron múltiples transformaciones específicas para estructurar y preparar los datos para el modelo dimensional:

✏️ Normalización y limpieza de columnas:
Renombrado de columnas a un formato estandarizado.

Conversión de strings a minúsculas para mantener consistencia.

Eliminación de espacios innecesarios y valores nulos en columnas críticas.

📊 Conversión y formateo de tipos:
Conversión de columnas categóricas a tipo category para optimizar memoria y análisis posterior.

Transformación de columnas temporales (crash_date, crash_time) en columnas individuales como:

crash_year, crash_month, crash_day, crash_hour, crash_day_of_week.

🔄 Unificación de categorías:
Homogeneización de valores con sinónimos o pequeñas variaciones (por ejemplo: "clear" y "Clear").

Estandarización de valores nulos representados de distintas formas ("unknown", "na", "not applicable" → np.nan).

💡 Generación de columnas adicionales:
Derivación de campos como:

is_weekend: indica si el accidente ocurrió un sábado o domingo.

is_night: según el crash_hour y la condición de iluminación (lighting_condition).

🧼 Filtrado de columnas:
Se eliminaron columnas irrelevantes o redundantes para análisis, como identificadores únicos no necesarios o metadata innecesaria.

El resultado fue un DataFrame listo para ser modelado en un esquema dimensional, almacenado también en formato .parquet.

3. Modelo Dimensional
Se diseñó un modelo estrella compuesto por:

🧩 Tabla de hechos: fact_accidents
Contiene los hechos principales del accidente, incluyendo las claves foráneas hacia las dimensiones y métricas clave como:

Severidad del accidente (most_severe_injury)

Tipo de colisión (crash_type)

Condición del conductor y causa principal (prim_contributory_cause)

📐 Tablas de dimensiones:
dim_date: contiene la dimensión temporal con columnas como año, mes, día, hora, día de la semana, y fin de semana.

dim_weather: estado del clima, visibilidad y condición de iluminación.

dim_location: tipo de intersección, tipo de vía, y superficie de la carretera.

dim_cause: causa principal del accidente y tipo de dispositivo de control de tráfico.

dim_injury_type: desglosa los distintos tipos de lesiones (heridos leves, graves, fallecidos, peatones, ciclistas, etc).

dim_traffic: tipo de accidente, tipo de tráfico y relación con intersecciones.

Cada tabla fue deduplicada y se les asignaron claves primarias enteras (IDs) para optimizar las relaciones con la tabla de hechos.

4. Carga
Todas las tablas generadas (hechos y dimensiones) fueron cargadas en una base de datos PostgreSQL.

Las tablas se crearon explícitamente si no existían, utilizando SQLAlchemy y Pandas con to_sql, manteniendo las relaciones del modelo estrella.

El esquema quedó listo para ser consultado desde Power BI u otra herramienta de visualización.
