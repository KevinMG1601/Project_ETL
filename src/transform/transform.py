import pandas as pd
import numpy as np

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones al DataFrame extraído:
    - Extrae el año y el día del mes desde la columna 'crash_date'.
    - Limpia y separa los valores de la columna 'damage' en 'damage_min' y 'damage_max'.
    - Rellena valores nulos en 'damage_min' con la moda y en 'damage_max' con la mediana.
    - Estandariza el texto de columnas categóricas como 'weather_condition', 'road_condition' y 'light_condition'.
    - Convierte 'age_of_driver' a valores numéricos enteros.
    - Elimina registros duplicados.
    - Elimina columnas innecesarias como 'crash_date' y 'damage'.

    Retorna:
        pd.DataFrame: DataFrame transformado y listo para la etapa de carga.
    """
    
    df["crash_year"] = pd.to_datetime(df["crash_date"], format="%Y-%m-%d").dt.year
    df["crash_day_of_month"] = pd.to_datetime(df["crash_date"], format="%Y-%m-%d").dt.day

    def limpiar_damage(valor):
        if pd.isna(valor):
            return np.nan, np.nan
        valor = valor.strip().upper()
        if "OVER" in valor:  
            return 1500, np.nan
        elif "$" in valor and "-" in valor:  
            valores = [int(v.replace("$", "").replace(",", "")) for v in valor.split(" - ")]
            return valores[0], valores[1]
        else: 
            return np.nan, np.nan

    df[["damage_min", "damage_max"]] = df["damage"].apply(lambda x: pd.Series(limpiar_damage(x)))

    mode_damage_min = df['damage_min'].mode()[0]
    df['damage_min'] = df['damage_min'].fillna(mode_damage_min)

    median_damage_max = df['damage_max'].median()
    df['damage_max'] = df['damage_max'].fillna(median_damage_max)

    cat_cols = ["weather_condition", "road_condition", "light_condition"]
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.lower()

    if "age_of_driver" in df.columns:
        df["age_of_driver"] = pd.to_numeric(df["age_of_driver"], errors="coerce").astype("Int64")

    df = df.drop_duplicates()

    columnas_eliminar = ["crash_date", "damage"]
    df = df.drop(columns=[col for col in columnas_eliminar if col in df.columns])

    return df
