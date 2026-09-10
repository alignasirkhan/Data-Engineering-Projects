\# Data Engineering Projects



A repository of end-to-end data engineering projects covering data pipelines, analytics, cloud platforms, data transformation, and modern data engineering technologies.



\## About This Repository



This repository contains practical data engineering projects designed to demonstrate the development of complete data platforms — from data ingestion and processing to storage, transformation, analytics, and visualization.



Each project is developed independently and includes its own documentation, source code, configuration, and supporting resources.



\## Projects



\### 1. WeatherLake Data Analytics Platform



An end-to-end weather data engineering platform built using Apache Airflow, Open-Meteo API, Parquet, PostgreSQL, and Power BI.



The platform implements a layered \*\*Bronze → Silver → Gold\*\* architecture for historical and real-time weather data processing.



\*\*Architecture:\*\*



```text

Open-Meteo API

&#x20;     ↓

&#x20;  Bronze

&#x20;Raw JSON Data

&#x20;     ↓

&#x20;  Silver

Cleaned Parquet

&#x20;     ↓

&#x20;   Gold

PostgreSQL Star Schema

&#x20;     ↓

&#x20;  Power BI

Analytics \& Dashboard

```



\*\*Technologies:\*\*



\* Python

\* Apache Airflow

\* Docker

\* Open-Meteo API

\* Apache Parquet

\* PostgreSQL

\* SQL

\* Power BI



\*\*Project:\*\*

\[WeatherLake Data Analytics Platform](./WeatherLake%20Data%20Analytics%20Platform/)



\## Repository Structure



```text

Data-Engineering-Projects/

│

├── README.md

│

└── WeatherLake Data Analytics Platform/

&#x20;   ├── README.md

&#x20;   ├── dags/

&#x20;   ├── config/

&#x20;   ├── sql/

&#x20;   ├── Dashboard/

&#x20;   ├── data/

&#x20;   ├── docker-compose.yml

&#x20;   └── requirements.txt

```



\## Goals



The projects in this repository are intended to demonstrate practical experience with:



\* Designing end-to-end data pipelines

\* Building batch and real-time data processing workflows

\* Implementing Bronze–Silver–Gold data architectures

\* Working with relational databases

\* Processing structured and semi-structured data

\* Developing analytics-ready datasets

\* Building data visualization solutions

\* Using orchestration tools such as Apache Airflow

\* Using Git and GitHub for version control

\* Developing reproducible data engineering projects



\## Author



\*\*Nasir Khan\*\*



Data Engineering | Data Analytics | Cloud \& Big Data



\---



⭐ This repository will be continuously updated as new data engineering projects are developed and added.



