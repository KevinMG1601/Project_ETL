import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from src.extract.extract import extract_data
from src.transform.transform import transform_data
from src.load.load import load_data

def main():
    print("🔍 Extrayendo datos...")
    df_raw = extract_data()
    print(df_raw.head())

    print("\n🧼 Transformando datos...")
    df_clean = transform_data(df_raw)
    print(df_clean.head())

    print("\n📦 Cargando datos a clean_accidents...")
    load_data(df_clean)
    print("✅ Carga completa.")

if __name__ == "__main__":
    main()
