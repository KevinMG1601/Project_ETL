import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv(override=True)

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
DATABASE = os.getenv("DB")

engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")

def get_data(query):
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

st.title("dashboard accidentes de trafico")

df = get_data("SELECT * FROM accidents;")

st.subheader("vist previa")
st.dataframe(df.head())

#funcion
def plot_column(df, col):
    st.subheader(f"estadisticas de la columna: {col}")
    if pd.api.types.is_numeric_dtype(df[col]):
        fig_hist = px.histogram(df, x=col, title=f'distribucion de {col}')
        st.plotly_chart(fig_hist)
        fig_box = px.box(df, y=col, title=f'box plot de {col}')
        st.plotly_chart(fig_box)
    else:
        counts = df[col].value_counts().reset_index()
        counts.columns = [col, 'count']
        fig_bar = px.bar(counts, x=col, y='count', title=f'frecuencia de {col}')
        st.plotly_chart(fig_bar)

for col in df.columns:
    if col == 'id':
        continue
    plot_column(df, col)
