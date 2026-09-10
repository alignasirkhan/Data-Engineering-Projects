"""
WeatherLake Near-Real-Time Data Pipeline
=========================================

Purpose:
    Fetch current weather for all configured cities every 15 minutes,
    store the raw response in Bronze, cleaned rows in Silver Parquet,
    and upsert the observations into the existing Gold fact table.

Historical data remains in WeatherLakeDataPlatform.py.
This DAG only handles current/near-real-time observations.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator


# ============================================================
# CONFIGURATION
# ============================================================

DAG_ID = "WeatherLakeRealtime"

BASE_DIR = Path("/opt/airflow/data")
REALTIME_BRONZE_DIR = BASE_DIR / "bronze" / "realtime_weather"
REALTIME_SILVER_DIR = BASE_DIR / "silver" / "realtime_weather"

CITIES_FILE = Path("/opt/airflow/config/cities.csv")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
GOLD_SCHEMA = "weatherlake_gold"

USER_AGENT = "WeatherLakeRealtime/1.0"
REQUEST_TIMEOUT_SECONDS = 120

CURRENT_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "weather_code",
    "cloud_cover",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
]

logger = logging.getLogger(__name__)


def ensure_directories():
    REALTIME_BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    REALTIME_SILVER_DIR.mkdir(parents=True, exist_ok=True)


def load_cities():
    if not CITIES_FILE.exists():
        raise FileNotFoundError(f"Cities file not found: {CITIES_FILE}")

    df = pd.read_csv(CITIES_FILE)
    required = {
        "location_id", "city", "district", "state", "country",
        "latitude", "longitude", "timezone",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"cities.csv missing columns: {sorted(missing)}")

    df["location_id"] = df["location_id"].astype(int)
    return df


def fetch_current_weather(cities):
    """Fetch current weather for all cities in one batched API request."""

    params = {
        "latitude": ",".join(cities["latitude"].astype(str)),
        "longitude": ",".join(cities["longitude"].astype(str)),
        "current": ",".join(CURRENT_VARIABLES),
        "timezone": "auto",
    }

    headers = {"User-Agent": USER_AGENT}

    logger.info("Fetching current weather for %s cities", len(cities))

    response = requests.get(
        OPEN_METEO_URL,
        params=params,
        headers=headers,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    payload = response.json()
    results = payload if isinstance(payload, list) else [payload]

    if len(results) != len(cities):
        raise ValueError(
            f"Expected {len(cities)} API results, received {len(results)}"
        )

    rows = []

    for city_row, result in zip(cities.itertuples(index=False), results):
        current = result.get("current") or {}
        timestamp = current.get("time")

        if not timestamp:
            logger.warning(
                "No current timestamp returned | location_id=%s",
                city_row.location_id,
            )
            continue

        row = {
            "location_id": int(city_row.location_id),
            "city": str(city_row.city),
            "district": str(city_row.district),
            "state": str(city_row.state),
            "country": str(city_row.country),
            "latitude": float(city_row.latitude),
            "longitude": float(city_row.longitude),
            "timezone": str(city_row.timezone),
            "timestamp": timestamp,
            "data_granularity": "realtime",
        }

        for variable in CURRENT_VARIABLES:
            row[variable] = current.get(variable)

        rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("Open-Meteo returned no current weather rows.")

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "location_id"])
    df = df.drop_duplicates(subset=["location_id", "timestamp"])

    return df, payload


def save_bronze(payload, run_timestamp):
    output = REALTIME_BRONZE_DIR / f"current_weather_{run_timestamp}.json"
    with open(output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    logger.info("Realtime Bronze saved: %s", output)
    return output


def save_silver(df, run_timestamp):
    output = REALTIME_SILVER_DIR / f"current_weather_{run_timestamp}.parquet"
    df.to_parquet(output, index=False, engine="pyarrow")
    logger.info("Realtime Silver saved: %s | rows=%s", output, len(df))
    return output


def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_dimensions(connection, df):
    """Insert current dates into dim_date; dim_time already contains 24 hours."""
    cursor = connection.cursor()

    date_rows = []
    for date_value in sorted(df["timestamp"].dt.date.unique()):
        ts = pd.Timestamp(date_value)
        date_rows.append(
            (
                int(ts.strftime("%Y%m%d")),
                date_value,
                ts.year,
                ts.quarter,
                ts.month,
                ts.strftime("%B"),
                ts.day,
                ts.strftime("%A"),
                ts.dayofweek + 1,
                int(ts.isocalendar().week),
            )
        )

    if date_rows:
        sql = f"""
            INSERT INTO {GOLD_SCHEMA}.dim_date
            (date_key, full_date, year, quarter, month, month_name,
             day, day_name, day_of_week, week_of_year)
            VALUES %s
            ON CONFLICT (date_key) DO UPDATE SET
                full_date=EXCLUDED.full_date,
                year=EXCLUDED.year,
                quarter=EXCLUDED.quarter,
                month=EXCLUDED.month,
                month_name=EXCLUDED.month_name,
                day=EXCLUDED.day,
                day_name=EXCLUDED.day_name,
                day_of_week=EXCLUDED.day_of_week,
                week_of_year=EXCLUDED.week_of_year;
        """
        execute_values(cursor, sql, date_rows, page_size=100)

    connection.commit()
    cursor.close()


def load_gold(df):
    connection = get_postgres_connection()

    try:
        cursor = connection.cursor()

        # Safe migration for an existing Gold table.
        cursor.execute(
            f"""
            ALTER TABLE {GOLD_SCHEMA}.fact_weather_hourly
            ADD COLUMN IF NOT EXISTS data_granularity VARCHAR(20)
            NOT NULL DEFAULT 'historical';
            """
        )
        connection.commit()
        cursor.close()

        ensure_dimensions(connection, df)

        rows = []
        for _, row in df.iterrows():
            ts = pd.Timestamp(row["timestamp"])

            def value(column):
                item = row.get(column)
                return None if pd.isna(item) else item

            weather_code = value("weather_code")
            if weather_code is not None:
                weather_code = int(weather_code)

            rows.append(
                (
                    int(row["location_id"]),
                    int(ts.strftime("%Y%m%d")),
                    int(ts.hour),
                    ts.to_pydatetime(),
                    "realtime",
                    value("temperature_2m"),
                    value("relative_humidity_2m"),
                    value("dew_point_2m"),
                    value("apparent_temperature"),
                    value("precipitation"),
                    value("rain"),
                    value("snowfall"),
                    weather_code,
                    value("cloud_cover"),
                    value("surface_pressure"),
                    value("wind_speed_10m"),
                    value("wind_direction_10m"),
                    value("wind_gusts_10m"),
                )
            )

        sql = f"""
            INSERT INTO {GOLD_SCHEMA}.fact_weather_hourly
            (
                location_id, date_key, time_key, timestamp, data_granularity,
                temperature_2m, relative_humidity_2m, dew_point_2m,
                apparent_temperature, precipitation, rain, snowfall,
                weather_code, cloud_cover, surface_pressure,
                wind_speed_10m, wind_direction_10m, wind_gusts_10m
            )
            VALUES %s
            ON CONFLICT (location_id, timestamp)
            DO UPDATE SET
                date_key = EXCLUDED.date_key,
                time_key = EXCLUDED.time_key,
                data_granularity = EXCLUDED.data_granularity,
                temperature_2m = EXCLUDED.temperature_2m,
                relative_humidity_2m = EXCLUDED.relative_humidity_2m,
                dew_point_2m = EXCLUDED.dew_point_2m,
                apparent_temperature = EXCLUDED.apparent_temperature,
                precipitation = EXCLUDED.precipitation,
                rain = EXCLUDED.rain,
                snowfall = EXCLUDED.snowfall,
                weather_code = EXCLUDED.weather_code,
                cloud_cover = EXCLUDED.cloud_cover,
                surface_pressure = EXCLUDED.surface_pressure,
                wind_speed_10m = EXCLUDED.wind_speed_10m,
                wind_direction_10m = EXCLUDED.wind_direction_10m,
                wind_gusts_10m = EXCLUDED.wind_gusts_10m;
        """

        cursor = connection.cursor()
        execute_values(cursor, sql, rows, page_size=500)
        connection.commit()
        cursor.close()

        logger.info("Realtime Gold loaded | rows=%s", len(rows))

    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def realtime_pipeline_task():
    logger.info("=" * 70)
    logger.info("WEATHERLAKE NEAR-REAL-TIME PIPELINE")
    logger.info("=" * 70)

    ensure_directories()
    cities = load_cities()

    run_timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    df, payload = fetch_current_weather(cities)
    save_bronze(payload, run_timestamp)
    save_silver(df, run_timestamp)
    load_gold(df)

    logger.info(
        "REALTIME PIPELINE COMPLETED | rows=%s | latest=%s",
        len(df),
        df["timestamp"].max(),
    )


# ============================================================
# AIRFLOW DAG
# ============================================================

default_args = {
    "owner": "WeatherLake",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id=DAG_ID,
    description="WeatherLake near-real-time current weather ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["WeatherLake", "Realtime", "Weather", "Open-Meteo"],
) as dag:

    realtime_weather = PythonOperator(
        task_id="RealtimeWeather",
        python_callable=realtime_pipeline_task,
    )
