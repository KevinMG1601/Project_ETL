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

st.set_page_config(page_title="Accident Dashboard", layout="wide")
st.title("Accident Analysis")

df = get_data("SELECT * FROM accidents;")
st.write("Number of records:", len(df))
st.subheader("Data preview")
st.dataframe(df.head())

#GRAFICAS 

st.subheader("Accident Traffic Control Graph")
query_traffic = """
SELECT traffic_control_device, COUNT(*) AS total 
FROM accidents 
GROUP BY traffic_control_device
"""
df_traffic = get_data(query_traffic)
fig_traffic = px.pie(
    df_traffic, 
    names='traffic_control_device', 
    values='total', 
    title="Distribution of Traffic Control Devices")
st.plotly_chart(fig_traffic)

st.subheader("Accidents by Weather Conditions")
query_weather = """
SELECT weather_condition, COUNT(*) AS total 
FROM accidents 
GROUP BY weather_condition
"""
df_weather = get_data(query_weather)
fig_weather = px.bar(
    df_weather, 
    x='weather_condition', 
    y='total', 
    title="Accidents by Weather Condition")
st.plotly_chart(fig_weather)

st.subheader("Lighting in Accidents")
query_lighting = """
SELECT lighting_condition, COUNT(*) AS total 
FROM accidents 
GROUP BY lighting_condition
"""
df_lighting = get_data(query_lighting)
fig_lighting = px.pie(
    df_lighting, 
    names='lighting_condition', 
    values='total', 
    title="Distribution of Lighting Conditions")
st.plotly_chart(fig_lighting)

st.subheader("Type of First Shock")
query_crash_type = """
SELECT first_crash_type, COUNT(*) AS total 
FROM accidents 
GROUP BY first_crash_type
"""
df_crash_type = get_data(query_crash_type)
fig_crash_type = px.bar(
    df_crash_type, 
    x='first_crash_type', 
    y='total', 
    title="Accidents by Type of First Crash")
st.plotly_chart(fig_crash_type)

st.subheader("Accidents by Type of Road")
query_trafficway = """
SELECT trafficway_type, COUNT(*) AS total 
FROM accidents 
GROUP BY trafficway_type
"""
df_trafficway = get_data(query_trafficway)
fig_trafficway = px.pie(
    df_trafficway, 
    names='trafficway_type', 
    values='total', 
    title="Accidents by Type of Road")
st.plotly_chart(fig_trafficway)

st.subheader("Road Surface Conditions")
query_surface = """
SELECT roadway_surface_cond, COUNT(*) AS total 
FROM accidents 
GROUP BY roadway_surface_cond
"""
df_surface = get_data(query_surface)
fig_surface = px.bar(
    df_surface, 
    x='roadway_surface_cond', 
    y='total', 
    title="Accidents due to Road Surface Conditions",
    color = "roadway_surface_cond")
st.plotly_chart(fig_surface)

st.subheader("Relationship with Intersections")
query_intersection = """
SELECT intersection_related_i, COUNT(*) AS total 
FROM accidents 
GROUP BY intersection_related_i
"""
df_intersection = get_data(query_intersection)
fig_intersection = px.pie(
    df_intersection, 
    names='intersection_related_i', 
    values='total', 
    title="Intersection-Related Accidents")
st.plotly_chart(fig_intersection)

st.subheader("Main Cause of Accident")
query_cause = """
SELECT prim_contributory_cause, COUNT(*) AS total 
FROM accidents 
GROUP BY prim_contributory_cause
"""
df_cause = get_data(query_cause)
fig_cause = px.bar(
    df_cause, 
    x='prim_contributory_cause', 
    y='total', 
    title="Causas Principales de los Accidentes")
st.plotly_chart(fig_cause)

st.subheader("Distribución de Lesiones")
query_injuries = """
SELECT most_severe_injury, COUNT(*) AS total 
FROM accidents 
GROUP BY most_severe_injury
"""
df_injuries = get_data(query_injuries)
fig_injuries = px.bar(
    df_injuries, 
    x='most_severe_injury', 
    y='total', 
    title="Tipos de Lesiones en los Accidentes",
    color= "most_severe_injury")
st.plotly_chart(fig_injuries)

st.subheader("Accidentes por Hora del Día")
query_hour = """
SELECT crash_hour, COUNT(*) AS total 
FROM accidents 
GROUP BY crash_hour
"""
df_hour = get_data(query_hour)
fig_hour = px.histogram(
    df_hour, 
    x='crash_hour', 
    y='total', 
    title="Distribución de Accidentes por Hora del Día")
st.plotly_chart(fig_hour)

st.subheader("Accidentes por Día de la Semana")
query_day = """
SELECT crash_day_of_week, COUNT(*) AS total 
FROM accidents 
GROUP BY crash_day_of_week
"""
df_day = get_data(query_day)
dias = {1: "Lunes", 2:"Martes", 3:"Miercoles", 4:"Jueves", 5:"Viernes", 6:"Sabado", 7:"Domingo"}

df_day["crash_day_of_week"] = df_day["crash_day_of_week"].map(dias)

fig_day = px.bar(
    df_day, 
    x='crash_day_of_week', 
    y='total', 
    title="Accidentes por Día de la Semana",
    color = 'crash_day_of_week',
    color_discrete_sequence=px.colors.qualitative.Pastel
    )
st.plotly_chart(fig_day)

st.subheader("Accidentes por Mes")
query_month = """
SELECT crash_month, COUNT(*) AS total 
FROM accidents 
GROUP BY crash_month
"""
df_month = get_data(query_month)
meses = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}
df_month["crash_month"] = df_month["crash_month"].map(meses)

fig_month = px.bar(
    df_month, 
    x='crash_month', 
    y='total', 
    title="Accidentes por Mes",
    color='crash_month',  
    color_discrete_sequence=px.colors.qualitative.Pastel
)
fig_month.update_layout(xaxis_title="Mes", yaxis_title="Cantidad de Accidentes")
st.plotly_chart(fig_month)




st.subheader("Accidentes por Año")
query_year = """
SELECT crash_year, COUNT(*) AS total 
FROM accidents 
GROUP BY crash_year
"""
df_year = get_data(query_year)

fig_year = px.line(
    df_year, 
    x='crash_year', 
    y='total', 
    title="Tendencia de Accidentes a lo Largo de los Años")
st.plotly_chart(fig_year)

