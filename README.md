# Project ETL - Traffic Accidents 🚗

## Overview
The objective of this project is to perform an in-depth exploratory analysis of the traffic accident dataset. The analysis seeks to identify patterns, time trends, geographic distribution, and relevant factors associated with the incidents, in order to generate knowledge that can support decision-making and prevention strategies.

## Dataset [Traffic Accidents Kaggle](https://www.kaggle.com/datasets/oktayrdeki/traffic-accidents)

* **crash_date:** The date the accident occurred.
* **traffic_control_device:** The type of traffic control device involved (e.g., traffic light, sign).
* **weather_condition:** The weather conditions at the time of the accident.
* **lighting_condition:** The lighting conditions at the time of the accident.
* **first_crash_type:** The initial type of the crash (e.g., head-on, rear-end).
* **trafficway_type:** The type of roadway involved in the accident (e.g., highway, local road).
* **alignment:** The alignment of the road where the accident occurred (e.g., straight, curved).
* **roadway_surface_cond:** The condition of the roadway surface (e.g., dry, wet, icy).
* **road_defect:** Any defects present on the road surface.
* **crash_type:** The overall type of the crash.
* **intersection_related_i:** Whether the accident was related to an intersection.
* **damage:** The extent of the damage caused by the accident.
* **prim_contributory_cause:** The primary cause contributing to the crash.
* **num_units:** The number of vehicles involved in the accident.
* **most_severe_injury:** The most severe injury sustained in the crash.
* **injuries_total:** The total number of injuries reported.
* **injuries_fatal:** The number of fatal injuries resulting from the accident.
* **injuries_incapacitating:** The number of incapacitating injuries.
* **injuries_non_incapacitating:** The number of non-incapacitating injuries.
* **injuries_reported_not_evident:** The number of injuries reported but not visibly evident.
* **injuries_no_indication:** The number of cases with no indication of injury.
* **crash_hour:** The hour the accident occurred.
* **crash_day_of_week:** The day of the week the accident occurred.
* **crash_month:** The month the accident occurred.

## Project Structure

| Folder/File   | Description   |
| ------------- |:-------------:|
| left foo      | right foo     |
| left bar      | right bar     |
| left baz      | right baz     |

## Tools and Libraries
El proyecto utiliza diversas herramientas de software para construir el flujo ETL, procesar los datos y modelarlos para análisis posterior:

* Python 3.11.9
* Apache Airflow – Orquestación de tareas ETL.
* Pandas – Manipulación y transformación de datos.
* SQLAlchemy – Conexión y ejecución de consultas SQL.
* MariaDB – Almacenamiento de datos crudos, limpios y modelo dimensional.
* dotenv – Gestión de variables de entorno.
* Kaggle API – Descarga de datasets desde la plataforma.

## Installation and Setup
1. Clona el repositorio:
    ```bash
    git clone https://github.com/KevinMG1601/Project_ETL.git
    cd Project_ETL
    ```

2. Crea y activa un entorno virtual:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. Instala las dependencias:
    ```bash
    pip install -r requirements.txt

## Configuración

### 1. Configura el archivo `.env`

* DB_USER = ""
* DB_PASSWORD = ""
* DB_PORT = ""
* DB_HOST = ""
* DB_RAW = "raw_accidents"
* DB_CLEAN = "clean_accidents"
* DB_MODEL = "model_accidents"

### 2. Airflow 

Ejecutar el primer code:
    ```bash
    export AIRFLOW_HOME="$(pwd)/airflow”

Despues ejecute:
    ```bash
    airflow standalone 

ya por ultimo vaya a localhost:8080 e inicia sesion con el password y user que te da airflow.

## Authors
#### Created by:

**Kevin Andres Muñoz G. - [Github](https://github.com/KevinMG1601) / [Linkedin](https://www.linkedin.com/in/kevin-mu%C3%B1oz-231b80303/)**


**Simon Garcia Rubiano. - [Github](https://github.com/simondev06) / [Linkedin]()**

