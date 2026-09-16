# Data Engineering Projects

A repository of end-to-end data engineering projects covering data ingestion, data pipelines, data transformation, cloud platforms, analytics, orchestration, and modern data engineering technologies.

These projects are built as practical implementations to demonstrate data engineering concepts across batch processing, real-time streaming, cloud data platforms, analytical modeling, and visualization.

---

## Projects

### 1. WeatherLake Data Analytics Platform

An end-to-end weather data engineering and analytics platform built around historical and real-time weather data.

**Key technologies:**

* Python
* Open-Meteo API
* Apache Airflow
* Docker
* PostgreSQL
* Apache Spark
* Parquet
* Power BI
* ETL / ELT
* Medallion-style data processing

**Key capabilities:**

* Historical weather data ingestion
* Real-time weather ingestion
* Automated ETL pipelines
* Data transformation and validation
* Analytical data modeling
* PostgreSQL-based analytics layer
* Interactive Power BI dashboard

[View WeatherLake Project](./WeatherLake%20Data%20Analytics%20Platform)

---

### 2. Real-Time Air Quality Index (AQI) Tracking & Analytics Platform

A real-time streaming data platform for collecting, processing, transforming, and visualizing air-quality data across multiple Indian cities.

**Key technologies:**

* Python
* Open-Meteo Air Quality API
* Azure Event Hubs
* Azure Databricks
* Apache Spark / Structured Streaming
* Delta Lake
* Unity Catalog
* Databricks SQL
* Grafana Cloud
* Azure Key Vault

**Architecture:**

```text
Open-Meteo Air Quality API
            │
            ▼
     Python Producer
            │
            ▼
     Azure Event Hubs
            │
            ▼
    Databricks Bronze
            │
            ▼
    Databricks Silver
            │
            ▼
      Databricks Gold
            │
            ▼
      Databricks SQL
            │
            ▼
       Grafana Cloud
```

**Key capabilities:**

* Real-time AQI event ingestion
* Azure Event Hubs streaming
* Databricks Structured Streaming
* Bronze → Silver → Gold processing
* Data validation and deduplication
* AQI category classification
* Dimensional analytical data model
* Automated Silver and Gold transformations
* Grafana Cloud monitoring dashboard
* Secure secret management with Azure Key Vault and Databricks Secret Scope

[View AQI Project](./Real-Time%20%28AQI%29%20Tracking%20%26%20Analytics%20Platform)

---

## Technology Areas

The projects in this repository demonstrate experience across several areas of modern data engineering:

| Area            | Technologies                    |
| --------------- | ------------------------------- |
| Programming     | Python, SQL                     |
| Data Ingestion  | REST APIs, Azure Event Hubs     |
| Processing      | Apache Spark, PySpark           |
| Streaming       | Structured Streaming            |
| Orchestration   | Apache Airflow                  |
| Storage         | PostgreSQL, Delta Lake, Parquet |
| Cloud           | Microsoft Azure                 |
| Data Governance | Unity Catalog                   |
| Analytics       | Databricks SQL, Power BI        |
| Visualization   | Power BI, Grafana Cloud         |
| Containers      | Docker                          |
| Version Control | Git, GitHub                     |

---

## Data Engineering Concepts

The projects cover practical implementation of:

* ETL / ELT pipelines
* Batch data processing
* Real-time data streaming
* Medallion architecture
* Data validation
* Data deduplication
* Dimensional data modeling
* Analytical data warehouses
* Cloud data platforms
* Workflow orchestration
* Data visualization
* Secret management
* Data quality monitoring

---

## Repository Structure

```text
Data-Engineering-Projects/
│
├── README.md
│
├── WeatherLake Data Analytics Platform/
│   ├── README.md
│   └── ...
│
└── Real-Time (AQI) Tracking & Analytics Platform/
    ├── README.md
    ├── producer.py
    ├── Dashboard/
    ├── Databricks/
    │   ├── Notebooks/
    │   └── SQL/
    ├── Architecture/
    ├── config/
    └── docs/
        └── screenshots/
```

---

## About

**Nasir Khan**

Data Engineering | Data Analytics | Cloud & Big Data

I build practical data engineering solutions focused on data ingestion, transformation, analytics, cloud platforms, and scalable data pipelines.

### Connect

* [LinkedIn](https://www.linkedin.com/in/alignasirkhan)
* [GitHub](https://github.com/alignasirkhan)

---

## Repository Purpose

This repository documents practical data engineering projects and implementations developed using modern data platforms and technologies.

Each project includes its own documentation, architecture, source code, data processing components, and analytics layer where applicable.
