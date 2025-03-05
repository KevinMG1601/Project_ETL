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

st.title("Dashboard de Contrataciones")

# Hires by technology (pie chart)
st.subheader("Contrataciones por Tecnología")
query_tech = """
SELECT technology, COUNT(*) AS hires 
FROM candidates 
WHERE code_challenge_score >= 7 AND technical_interview_score >= 7
GROUP BY technology
"""
df_tech = get_data(query_tech)
fig_tech = px.pie(
    df_tech, 
    names='technology', 
    values='hires', 
    labels={"technology":"tegnologia", "hires":"contrataciones"})
st.plotly_chart(fig_tech)

#Hires by year (horizontal bar chart)
st.subheader("Contrataciones por Año")
query_year = """
SELECT YEAR(application_date) AS year, COUNT(*) AS hires 
FROM candidates 
WHERE code_challenge_score >= 7 AND technical_interview_score >= 7
GROUP BY year
ORDER BY year
"""
df_year = get_data(query_year)
fig_year = px.bar(
    df_year, 
    x='year', 
    y='hires',
    color="year", 
    labels={"year":"año", "hires":"contrataciones"})
st.plotly_chart(fig_year)

#Hires by seniority (bar chart)
st.subheader("Contrataciones por antiguedad")
query_seniority = """
SELECT seniority, COUNT(*) AS hires 
FROM candidates 
WHERE code_challenge_score >= 7 AND technical_interview_score >= 7
GROUP BY seniority
"""
df_seniority = get_data(query_seniority)
fig_seniority = px.bar(
    df_seniority, 
    x='seniority', 
    y='hires',
    color="seniority",
    labels={"seniority":"antiguedad", "hires":"contrataciones"})
st.plotly_chart(fig_seniority)

#Hires por País (Multiline Chart)
st.subheader("Contrataciones por País a lo largo del Tiempo")
query_country = """
SELECT YEAR(application_date) AS year, country, COUNT(*) AS hires
FROM candidates
WHERE country IN ('USA', 'Brazil', 'Colombia', 'Ecuador')
AND code_challenge_score >= 7 AND technical_interview_score >= 7
GROUP BY year, country
ORDER BY year
"""
df_country = get_data(query_country)
fig_country = px.line(
    df_country, 
    x="year", 
    y="hires", 
    color="country", 
    labels={"year":"año", "hires":"contrataciones", "country":"Pais"})

st.plotly_chart(fig_country)

