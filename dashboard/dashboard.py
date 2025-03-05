import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
DATABASE = os.getenv("DB") 

engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")

def get_data(query):
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

st.title("Dashboard de Accidentes de Tráfico")

#obtener datos
df = get_data("SELECT * FROM accidents;")

st.write("Cantidad de registros:", len(df))
st.subheader("Vista previa de datos")
st.dataframe(df.head())

chart_mapping = {
    'traffic_control_device': 'pie',
    'weather_condition': 'bar',
    'lighting_condition': 'pie',
    'first_crash_type': 'bar',
    'trafficway_type': 'pie',
    'roadway_surface_cond': 'bar',
    'crash_type': 'bar',
    'intersection_related_i': 'pie',
    'prim_contributory_cause': 'bar',
    'most_severe_injury': 'bar',
    'crash_hour': 'histogram',
    'crash_day_of_week': 'bar',
    'crash_month': 'bar',
    'crash_year': 'line'
}

def plot_chart(df, col, chart_type):
    st.subheader(f"Grafica de {col}")
    if chart_type == 'pie':
        counts = df[col].value_counts().reset_index()
        counts.columns = [col, 'count']
        fig = px.pie(counts, names=col, values='count', title=f'{col} -Torta')
        st.plotly_chart(fig)
    elif chart_type == 'bar':
        counts = df[col].value_counts().reset_index()
        counts.columns = [col, 'count']
        fig = px.bar(counts, x=col, y='count', title=f'{col} - Barras')
        st.plotly_chart(fig)
    elif chart_type == 'histogram':
        fig = px.histogram(df, x=col, title=f'{col} - Histograma')
        st.plotly_chart(fig)
    elif chart_type == 'line':
        temp = df.groupby(col).size().reset_index(name='count')
        fig = px.line(temp, x=col, y='count', title=f'{col} - Lineal')
        st.plotly_chart(fig)

for col, chart_type in chart_mapping.items():
    if col in df.columns:
        plot_chart(df, col, chart_type)
