import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformaciones al DataFrame extraído.
    """

    # Ejemplo de limpieza: eliminar filas con valores nulos
    df_clean = df.dropna()


    return df_clean
