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

st.set_page_config(page_title="Accident Dashboard", layout="wide")
st.title("Traffic Accident Analysis")

query = """
SELECT * FROM accidents_clean
"""

@st.cache_data
def get_data():
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

<<<<<<< HEAD
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
=======
df = get_data()

st.write("Total records:", len(df))
st.subheader("Data Preview")
st.dataframe(df.head())

st.subheader("Traffic Control Devices")
df_traffic = df.groupby("traffic_control_device", as_index=False).size()
fig_traffic = px.pie(df_traffic, names='traffic_control_device', values='size', 
                     title="Distribution of Traffic Control Devices")
st.plotly_chart(fig_traffic)

st.subheader("Weather Conditions")
df_weather = df.groupby("weather_condition", as_index=False).size()
fig_weather = px.bar(df_weather, x='weather_condition', y='size', 
                     title="Accidents by Weather Condition")
st.plotly_chart(fig_weather)

st.subheader("Lighting Conditions")
df_lighting = df.groupby("lighting_condition", as_index=False).size()
fig_lighting = px.pie(df_lighting, names='lighting_condition', values='size', 
                      title="Distribution of Lighting Conditions")
st.plotly_chart(fig_lighting)

st.subheader("First Crash Type")
df_crash_type = df.groupby("first_crash_type", as_index=False).size()
fig_crash_type = px.bar(df_crash_type, x='first_crash_type', y='size', 
                        title="Accidents by First Crash Type")
st.plotly_chart(fig_crash_type)

st.subheader("Road Type")
df_trafficway = df.groupby("trafficway_type", as_index=False).size()
fig_trafficway = px.pie(df_trafficway, names='trafficway_type', values='size', 
                        title="Accidents by Road Type")
st.plotly_chart(fig_trafficway)

st.subheader("Road Surface Conditions")
df_surface = df.groupby("roadway_surface_cond", as_index=False).size()
fig_surface = px.bar(df_surface, x='roadway_surface_cond', y='size', 
                     title="Accidents by Road Surface Condition", color="roadway_surface_cond")
st.plotly_chart(fig_surface)

st.subheader("Intersection Related Accidents")
df_intersection = df.groupby("intersection_related_i", as_index=False).size()
fig_intersection = px.pie(df_intersection, names='intersection_related_i', values='size', 
                          title="Accidents Related to Intersections")
st.plotly_chart(fig_intersection)

st.subheader("Primary Contributory Causes")
df_cause = df.groupby("prim_contributory_cause", as_index=False).size()
fig_cause = px.bar(df_cause, x='prim_contributory_cause', y='size', 
                   title="Primary Causes of Accidents")
>>>>>>> main
st.plotly_chart(fig_cause)

st.subheader("Injury Severity Distribution")
df_injuries = df.groupby("most_severe_injury", as_index=False).size()
fig_injuries = px.bar(df_injuries, x='most_severe_injury', y='size', 
                      title="Types of Injuries in Accidents", color="most_severe_injury")
st.plotly_chart(fig_injuries)

st.subheader("Accidents by Hour")
df_hour = df.groupby("crash_hour", as_index=False).size()
fig_hour = px.histogram(df_hour, x='crash_hour', y='size', 
                        title="Accident Distribution by Hour of the Day")
st.plotly_chart(fig_hour)

st.subheader("Accidents by Day of the Week")
days = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}
df["crash_day_of_week"] = df["crash_day_of_week"].map(days)
df_day = df.groupby("crash_day_of_week", as_index=False).size()
fig_day = px.bar(df_day, x='crash_day_of_week', y='size', 
                 title="Accidents by Day of the Week", color='crash_day_of_week', 
                 color_discrete_sequence=px.colors.qualitative.Pastel)
st.plotly_chart(fig_day)

st.subheader("Accidents by Month")
months = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
}
df["crash_month"] = df["crash_month"].map(months)
df_month = df.groupby("crash_month", as_index=False).size()
fig_month = px.bar(df_month, x='crash_month', y='size', 
                   title="Accidents by Month", color='crash_month',  
                   color_discrete_sequence=px.colors.qualitative.Pastel)
fig_month.update_layout(xaxis_title="Month", yaxis_title="Number of Accidents")
st.plotly_chart(fig_month)

st.subheader("Accidents by Year")
df_year = df.groupby("crash_year", as_index=False).size()
fig_year = px.line(df_year, x='crash_year', y='size', 
                   title="Accident Trends Over the Years")
st.plotly_chart(fig_year)