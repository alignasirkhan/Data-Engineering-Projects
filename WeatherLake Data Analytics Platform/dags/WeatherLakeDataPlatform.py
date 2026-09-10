"""
WeatherLake Data Platform
=========================

Architecture:

Open-Meteo API
      |
      v
BronzeLayer  -> Raw Open-Meteo JSON
      |
      v
SilverLayer  -> Cleaned / Validated Parquet
      |
      v
GoldLayer    -> PostgreSQL Star Schema
      |
      v
Power BI

DAG ID:
    WeatherLakeDataPlatform
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
import unicodedata
from datetime import datetime, timedelta
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

DAG_ID = "WeatherLakeDataPlatform"

BASE_DIR = Path("/opt/airflow/data")

BRONZE_DIR = BASE_DIR / "bronze" / "weather"
SILVER_DIR = BASE_DIR / "silver" / "weather"
GOLD_DIR = BASE_DIR / "gold" / "weather"

CITIES_FILE = Path("/opt/airflow/config/cities.csv")

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"

GOLD_SCHEMA = "weatherlake_gold"


# ============================================================
# PIPELINE DATE RANGE
# ============================================================

START_YEAR = 2020
END_YEAR = 2026

# Historical data ends at August 31, 2026
FINAL_DATE_2026 = "2026-08-31"

# Incremental ingestion: fetch up to yesterday so the current partial day
# is not loaded as an incomplete hourly partition.
INCREMENTAL_END_LAG_DAYS = 1


# ============================================================
# TEST MODE
# ============================================================
#
# KEEP THIS TRUE while testing.
#
# It will request:
#     Location 1 = Mumbai
#     Year 2020
#
# After successful testing:
#
#     TEST_MODE = False
#
# Then the pipeline processes all 100 cities.
#

TEST_MODE = False

TEST_LOCATION_IDS = [1]

TEST_YEARS = [2020]


# ============================================================
# OPEN-METEO VARIABLES
# ============================================================

HOURLY_VARIABLES = [
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


# ============================================================
# API SETTINGS
# ============================================================

REQUEST_DELAY_SECONDS = 15

MAX_RETRIES = 3

INITIAL_RETRY_DELAY_SECONDS = 60

MAX_RETRY_DELAY_SECONDS = 300

REQUEST_TIMEOUT_SECONDS = 120

STOP_ON_DAILY_LIMIT = True

USER_AGENT = "WeatherLakeDataPlatform/1.0"


# ============================================================
# LOGGING
# ============================================================

logger = logging.getLogger(__name__)


# ============================================================
# CUSTOM EXCEPTION
# ============================================================

class DailyLimitReached(Exception):
    """Raised when Open-Meteo daily API quota is exhausted."""


# ============================================================
# GENERAL HELPERS
# ============================================================

def ensure_directories():
    """Create required pipeline directories."""

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    GOLD_DIR.mkdir(parents=True, exist_ok=True)


def load_cities():
    """Load and validate city configuration."""

    logger.info("Loading city configuration from: %s", CITIES_FILE)

    if not CITIES_FILE.exists():
        raise FileNotFoundError(
            f"Cities file not found: {CITIES_FILE}"
        )

    cities = pd.read_csv(CITIES_FILE)

    logger.info("Cities loaded from CSV: %s", len(cities))

    required_columns = [
        "location_id",
        "city",
        "district",
        "state",
        "country",
        "latitude",
        "longitude",
        "timezone",
    ]

    missing_columns = [
        col for col in required_columns
        if col not in cities.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing columns in cities.csv: {missing_columns}"
        )

    if cities["location_id"].duplicated().any():
        duplicates = cities[
            cities["location_id"].duplicated(keep=False)
        ]["location_id"].tolist()

        raise ValueError(
            f"Duplicate location_id values found: {duplicates}"
        )

    if cities[required_columns].isnull().any().any():
        null_counts = cities[required_columns].isnull().sum()

        raise ValueError(
            f"Null values found in cities.csv:\n{null_counts}"
        )

    logger.info("City configuration validation: PASSED")

    return cities


# ============================================================
# BRONZE LAYER
# ============================================================

def get_year_dates(year: int):
    """Return API start/end dates for a given year."""

    start_date = f"{year}-01-01"

    if year == 2026:
        end_date = FINAL_DATE_2026
    else:
        end_date = f"{year}-12-31"

    return start_date, end_date


def expected_hours_for_year(year: int) -> int:
    """Calculate expected hourly records."""

    start_date = pd.Timestamp(f"{year}-01-01")

    if year == 2026:
        end_date = pd.Timestamp(FINAL_DATE_2026)
    else:
        end_date = pd.Timestamp(f"{year}-12-31")

    days = (end_date - start_date).days + 1

    return days * 24


def get_bronze_partition(location_id: int, year: int):
    """Return Bronze partition path."""

    return (
        BRONZE_DIR
        / f"location_id={location_id}"
        / f"year={year}"
    )


def bronze_files(location_id: int, year: int):
    """Return Bronze raw and metadata paths."""

    partition = get_bronze_partition(location_id, year)

    return (
        partition / "weather_raw.json",
        partition / "metadata.json",
    )


def is_bronze_complete(location_id: int, year: int):
    """
    Verify whether a Bronze partition already exists
    and contains the expected number of hourly records.
    """

    raw_file, metadata_file = bronze_files(
        location_id,
        year
    )

    if not raw_file.exists() or not metadata_file.exists():
        return False

    try:
        with open(metadata_file, "r", encoding="utf-8") as f:
            metadata = json.load(f)

        expected = expected_hours_for_year(year)

        metadata_count = metadata.get("record_count", 0)

        if metadata_count != expected:
            return False

        with open(raw_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        actual_count = len(
            raw_data.get("hourly", {}).get("time", [])
        )

        return actual_count == expected

    except Exception:
        return False


def fetch_year(
    session,
    city_row,
    year: int,
    start_date_override=None,
    end_date_override=None,
):
    """Fetch a city/year or incremental date range from Open-Meteo."""

    location_id = int(city_row["location_id"])
    city = str(city_row["city"])
    latitude = float(city_row["latitude"])
    longitude = float(city_row["longitude"])
    timezone = str(city_row["timezone"])

    if start_date_override and end_date_override:
        start_date = str(start_date_override)
        end_date = str(end_date_override)
    else:
        start_date, end_date = get_year_dates(year)

    requested_start = pd.Timestamp(start_date)
    requested_end = pd.Timestamp(end_date)
    expected = (requested_end - requested_start).days * 24 + 24

    logger.info(
        "API request | City=%s | Location=%s | Range=%s to %s",
        city, location_id, start_date, end_date,
    )

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": timezone,
    }

    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                OPEN_METEO_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                try:
                    error_data = response.json()
                except Exception:
                    error_data = {}

                reason = str(error_data.get("reason", response.text)).lower()
                if "daily" in reason and "limit" in reason:
                    logger.error("OPEN-METEO DAILY API LIMIT REACHED.")
                    raise DailyLimitReached(
                        "Open-Meteo daily API limit reached."
                    )

                if attempt < MAX_RETRIES:
                    retry_delay = min(
                        INITIAL_RETRY_DELAY_SECONDS * (2 ** (attempt - 1)),
                        MAX_RETRY_DELAY_SECONDS,
                    )
                    time.sleep(retry_delay)
                    continue
                response.raise_for_status()

            response.raise_for_status()
            data = response.json()
            hourly = data.get("hourly")

            if not hourly:
                raise ValueError("API response does not contain hourly data.")

            times = hourly.get("time", [])
            actual = len(times)

            if actual == 0:
                raise ValueError(
                    f"No hourly data returned for {city} {start_date} to {end_date}."
                )

            # Historical/full-year requests must be complete. Incremental requests
            # are allowed to return fewer rows if the provider has not published
            # every requested recent hour yet.
            if not (start_date_override and end_date_override) and actual != expected:
                raise ValueError(
                    f"Incomplete API response for {city} {year}. "
                    f"Expected={expected}, Actual={actual}"
                )

            logger.info(
                "API success | City=%s | Range=%s to %s | Records=%s",
                city, start_date, end_date, actual,
            )
            return data

        except DailyLimitReached:
            raise
        except requests.RequestException as exc:
            logger.warning(
                "Request failed | City=%s | Range=%s to %s | Attempt=%s/%s | Error=%s",
                city, start_date, end_date, attempt, MAX_RETRIES, exc,
            )
            if attempt == MAX_RETRIES:
                raise
            retry_delay = min(
                INITIAL_RETRY_DELAY_SECONDS * (2 ** (attempt - 1)),
                MAX_RETRY_DELAY_SECONDS,
            )
            time.sleep(retry_delay)
        except Exception:
            raise

    raise RuntimeError(f"Failed to retrieve {city} {start_date} to {end_date}")


def get_latest_bronze_timestamp(location_id: int, year: int):
    """Return the latest hourly timestamp already stored in Bronze."""
    raw_file, metadata_file = bronze_files(location_id, year)
    if not raw_file.exists() or not metadata_file.exists():
        return None

    try:
        with open(raw_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        times = raw_data.get("hourly", {}).get("time", [])
        if not times:
            return None
        return pd.to_datetime(times).max()
    except Exception:
        return None


def append_bronze(
    city_row,
    year: int,
    incremental_data,
    incremental_start,
    incremental_end,
):
    """Append incremental API rows to an existing Bronze annual partition."""
    location_id = int(city_row["location_id"])
    city = str(city_row["city"])
    raw_file, metadata_file = bronze_files(location_id, year)

    with open(raw_file, "r", encoding="utf-8") as f:
        existing = json.load(f)

    existing_hourly = existing.setdefault("hourly", {})
    new_hourly = incremental_data.get("hourly", {})
    existing_times = existing_hourly.get("time", [])

    # Append and deduplicate by timestamp while keeping all weather variables aligned.
    combined_times = existing_times + new_hourly.get("time", [])
    order = sorted(range(len(combined_times)), key=lambda i: combined_times[i])
    unique_indices = []
    seen = set()
    for i in order:
        ts = combined_times[i]
        if ts not in seen:
            seen.add(ts)
            unique_indices.append(i)

    for key, values in list(existing_hourly.items()):
        if key == "time":
            continue
        new_values = new_hourly.get(key, [])
        combined_values = values + new_values
        existing_hourly[key] = [combined_values[i] for i in unique_indices]
    existing_hourly["time"] = [combined_times[i] for i in unique_indices]

    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False)

    metadata = {
        "location_id": location_id,
        "city": city,
        "district": str(city_row["district"]),
        "state": str(city_row["state"]),
        "country": str(city_row["country"]),
        "latitude": float(city_row["latitude"]),
        "longitude": float(city_row["longitude"]),
        "timezone": str(city_row["timezone"]),
        "year": year,
        "start_date": str(pd.to_datetime(existing_hourly["time"]).min().date()),
        "end_date": str(pd.to_datetime(existing_hourly["time"]).max().date()),
        "record_count": len(existing_hourly["time"]),
        "last_incremental_start": str(incremental_start),
        "last_incremental_end": str(incremental_end),
        "ingested_at_utc": datetime.utcnow().isoformat(),
        "source": "Open-Meteo Historical API",
        "api_url": OPEN_METEO_URL,
    }

    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    logger.info(
        "Bronze incremental update | City=%s | Year=%s | Total records=%s | End=%s",
        city, year, metadata["record_count"], metadata["end_date"],
    )

def save_bronze(
    city_row,
    year: int,
    data
):
    """Save raw API response to Bronze."""

    location_id = int(city_row["location_id"])

    city = str(city_row["city"])

    partition = get_bronze_partition(
        location_id,
        year
    )

    partition.mkdir(
        parents=True,
        exist_ok=True
    )

    raw_file, metadata_file = bronze_files(
        location_id,
        year
    )

    record_count = len(
        data["hourly"]["time"]
    )

    metadata = {
        "location_id": location_id,
        "city": city,
        "district": str(city_row["district"]),
        "state": str(city_row["state"]),
        "country": str(city_row["country"]),
        "latitude": float(city_row["latitude"]),
        "longitude": float(city_row["longitude"]),
        "timezone": str(city_row["timezone"]),
        "year": year,
        "start_date": get_year_dates(year)[0],
        "end_date": get_year_dates(year)[1],
        "record_count": record_count,
        "ingested_at_utc": datetime.utcnow().isoformat(),
        "source": "Open-Meteo Historical API",
        "api_url": OPEN_METEO_URL,
    }

    with open(
        raw_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
        )

    with open(
        metadata_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            metadata,
            f,
            indent=2,
            ensure_ascii=False,
        )

    logger.info(
        "Bronze saved | City=%s | Year=%s | Records=%s",
        city,
        year,
        record_count,
    )


def bronze_layer_task():
    """Bronze ingestion with historical backfill plus incremental updates."""

    logger.info("=" * 70)
    logger.info("WEATHERLAKE BRONZELAYER")
    logger.info("=" * 70)

    ensure_directories()
    cities = load_cities()
    session = requests.Session()

    # Keep the existing test switch for initial testing.
    if TEST_MODE:
        logger.info("TEST MODE ENABLED")
        for location_id in TEST_LOCATION_IDS:
            matching = cities[cities["location_id"] == location_id]
            if matching.empty:
                raise ValueError(f"Location ID {location_id} not found in cities.csv")
            city_row = matching.iloc[0]
            for year in TEST_YEARS:
                if is_bronze_complete(location_id, year):
                    logger.info("Bronze already complete | Location=%s | Year=%s", location_id, year)
                    continue
                data = fetch_year(session, city_row, year)
                save_bronze(city_row, year, data)
                time.sleep(REQUEST_DELAY_SECONDS)
        logger.info("TEST BRONZE LAYER COMPLETED")
        return

    logger.info("FULL / INCREMENTAL MODE ENABLED")
    total_requests = 0
    today = pd.Timestamp(datetime.now().date())
    incremental_end = today - pd.Timedelta(days=INCREMENTAL_END_LAG_DAYS)

    for _, city_row in cities.iterrows():
        location_id = int(city_row["location_id"])
        city = str(city_row["city"])

        # Historical years remain protected by the existing completeness check.
        for year in range(START_YEAR, END_YEAR + 1):
            if year < END_YEAR:
                if is_bronze_complete(location_id, year):
                    continue
                data = fetch_year(session, city_row, year)
                save_bronze(city_row, year, data)
                total_requests += 1
                time.sleep(REQUEST_DELAY_SECONDS)
                continue

            # Current year: backfill the original historical partition if missing.
            latest = get_latest_bronze_timestamp(location_id, year)

            if latest is None:
                data = fetch_year(session, city_row, year)
                save_bronze(city_row, year, data)
                total_requests += 1
                time.sleep(REQUEST_DELAY_SECONDS)
                latest = get_latest_bronze_timestamp(location_id, year)

            if latest is None:
                raise RuntimeError(f"Unable to determine latest Bronze timestamp for {city} ({location_id}).")

            next_date = latest.normalize() + pd.Timedelta(days=1)

            if next_date <= incremental_end:
                start_date = next_date.strftime("%Y-%m-%d")
                end_date = incremental_end.strftime("%Y-%m-%d")

                logger.info(
                    "Incremental extraction | City=%s | Location=%s | Range=%s to %s",
                    city, location_id, start_date, end_date,
                )

                data = fetch_year(
                    session, city_row, year,
                    start_date_override=start_date,
                    end_date_override=end_date,
                )

                append_bronze(
                    city_row, year, data, start_date, end_date
                )
                total_requests += 1
                time.sleep(REQUEST_DELAY_SECONDS)
            else:
                logger.info(
                    "Bronze is current | City=%s | Latest=%s",
                    city, latest,
                )

    logger.info(
        "BRONZE LAYER COMPLETED | API requests=%s | Incremental end=%s",
        total_requests, incremental_end.date(),
    )


# ============================================================
# SILVER LAYER
# ============================================================

def remove_accents(value):
    """
    Remove Unicode accents / diacritics.

    Examples:

        Bengaluru -> Bengaluru
        ā         -> a
        ī         -> i
        ū         -> u
        São       -> Sao
    """

    if pd.isna(value):
        return value

    value = str(value)

    normalized = unicodedata.normalize(
        "NFKD",
        value
    )

    cleaned = "".join(
        char
        for char in normalized
        if not unicodedata.combining(char)
    )

    return cleaned


def clean_text(value):
    """Standardize text values."""

    if pd.isna(value):
        return None

    value = remove_accents(value)

    value = " ".join(
        str(value).strip().split()
    )

    return value


def validate_weather_ranges(df):
    """Validate physical measurement ranges."""

    range_rules = {

        "relative_humidity_2m": (
            0,
            100
        ),

        "cloud_cover": (
            0,
            100
        ),

        "wind_direction_10m": (
            0,
            360
        ),

        "wind_speed_10m": (
            0,
            150
        ),

        "wind_gusts_10m": (
            0,
            200
        ),

        "precipitation": (
            0,
            1000
        ),

        "rain": (
            0,
            1000
        ),

        "snowfall": (
            0,
            1000
        ),

        "surface_pressure": (
            500,
            1200
        ),

        "weather_code": (
            0,
            99
        ),
    }

    for column, (minimum, maximum) in range_rules.items():

        if column not in df.columns:
            continue

        invalid = (
            df[column].notna()
            &
            (
                (df[column] < minimum)
                |
                (df[column] > maximum)
            )
        )

        invalid_count = int(
            invalid.sum()
        )

        if invalid_count > 0:

            logger.warning(
                "Range validation | "
                "Column=%s | Invalid=%s",
                column,
                invalid_count,
            )

            df.loc[
                invalid,
                column
            ] = pd.NA

    return df


def process_bronze_file(
    raw_file,
    metadata_file
):
    """Transform one Bronze JSON file into Silver Parquet."""

    with open(
        raw_file,
        "r",
        encoding="utf-8"
    ) as f:

        raw_data = json.load(f)

    with open(
        metadata_file,
        "r",
        encoding="utf-8"
    ) as f:

        metadata = json.load(f)

    hourly = raw_data.get(
        "hourly",
        {}
    )

    timestamps = hourly.get(
        "time",
        []
    )

    if not timestamps:
        raise ValueError(
            f"No hourly timestamps found: {raw_file}"
        )

    data = {
        "timestamp": timestamps
    }

    for variable in HOURLY_VARIABLES:

        values = hourly.get(
            variable,
            []
        )

        data[variable] = values

    df = pd.DataFrame(data)

    # --------------------------------------------------------
    # Add location attributes
    # --------------------------------------------------------

    df["location_id"] = int(
        metadata["location_id"]
    )

    df["city"] = metadata["city"]

    df["district"] = metadata["district"]

    df["state"] = metadata["state"]

    df["country"] = metadata["country"]

    df["latitude"] = float(
        metadata["latitude"]
    )

    df["longitude"] = float(
        metadata["longitude"]
    )

    df["timezone"] = metadata["timezone"]

    df["year"] = int(
        metadata["year"]
    )

    # --------------------------------------------------------
    # Timestamp validation
    # --------------------------------------------------------

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce"
    )

    before = len(df)

    df = df.dropna(
        subset=[
            "timestamp",
            "location_id"
        ]
    )

    logger.info(
        "Timestamp validation | "
        "Removed=%s",
        before - len(df),
    )

    # --------------------------------------------------------
    # Numeric type conversion
    # --------------------------------------------------------

    numeric_columns = [
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
        "latitude",
        "longitude",
    ]

    for column in numeric_columns:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    # --------------------------------------------------------
    # Clean text
    # --------------------------------------------------------

    text_columns = [
        "city",
        "district",
        "state",
        "country",
        "timezone",
    ]

    for column in text_columns:

        df[column] = df[column].apply(
            clean_text
        )

    # --------------------------------------------------------
    # Null handling
    # --------------------------------------------------------
    #
    # Precipitation variables represent measured amounts.
    # Missing values are treated as zero where appropriate.
    #

    zero_fill_columns = [
        "precipitation",
        "rain",
        "snowfall",
    ]

    for column in zero_fill_columns:

        if column in df.columns:

            df[column] = df[column].fillna(
                0
            )

    # --------------------------------------------------------
    # Measurement range validation
    # --------------------------------------------------------

    df = validate_weather_ranges(
        df
    )

    # --------------------------------------------------------
    # Deduplication
    # --------------------------------------------------------

    before = len(df)

    df = df.drop_duplicates(
        subset=[
            "location_id",
            "timestamp",
        ]
    )

    logger.info(
        "Deduplication | Removed=%s",
        before - len(df),
    )

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    df = df.sort_values(
        [
            "location_id",
            "timestamp",
        ]
    ).reset_index(
        drop=True
    )

    # --------------------------------------------------------
    # Final column order
    # --------------------------------------------------------

    final_columns = [
        "location_id",
        "city",
        "district",
        "state",
        "country",
        "latitude",
        "longitude",
        "timezone",
        "timestamp",
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
        "year",
    ]

    df = df[
        [
            column
            for column in final_columns
            if column in df.columns
        ]
    ]

    return df


def silver_layer_task():
    """
    SilverLayer

    Responsibilities:
    - Read Bronze JSON
    - Flatten API structure
    - Clean text
    - Remove Unicode accents
    - Handle nulls
    - Convert data types
    - Validate timestamps
    - Validate measurement ranges
    - Remove duplicates
    - Write cleaned Parquet
    """

    logger.info("=" * 70)
    logger.info("WEATHERLAKE SILVERLAYER")
    logger.info("=" * 70)

    ensure_directories()

    if not BRONZE_DIR.exists():
        raise FileNotFoundError(
            "Bronze directory does not exist."
        )

    bronze_files_found = list(
        BRONZE_DIR.rglob(
            "weather_raw.json"
        )
    )

    if not bronze_files_found:

        raise FileNotFoundError(
            "No Bronze weather files found."
        )

    total_rows = 0
    processed_files = 0

    for raw_file in bronze_files_found:

        metadata_file = (
            raw_file.parent
            / "metadata.json"
        )

        if not metadata_file.exists():

            logger.warning(
                "Metadata missing: %s",
                metadata_file,
            )

            continue

        df = process_bronze_file(
            raw_file,
            metadata_file,
        )

        if df.empty:

            logger.warning(
                "Silver dataframe empty: %s",
                raw_file,
            )

            continue

        location_id = int(
            df["location_id"].iloc[0]
        )

        year = int(
            df["year"].iloc[0]
        )

        partition = (
            SILVER_DIR
            / f"location_id={location_id}"
            / f"year={year}"
        )

        partition.mkdir(
            parents=True,
            exist_ok=True
        )

        output_file = (
            partition
            / "weather_cleaned.parquet"
        )

        df.to_parquet(
            output_file,
            index=False,
            engine="pyarrow",
        )

        logger.info(
            "Silver saved | "
            "Location=%s | Year=%s | Rows=%s",
            location_id,
            year,
            len(df),
        )

        total_rows += len(df)

        processed_files += 1

    logger.info(
        "SILVER LAYER COMPLETED | "
        "Files=%s | Rows=%s",
        processed_files,
        total_rows,
    )


# ============================================================
# GOLD LAYER
# ============================================================

def get_postgres_connection():
    """Create PostgreSQL connection."""

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DATABASE,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def create_gold_schema(connection):
    """Create Gold star schema."""

    cursor = connection.cursor()

    cursor.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};
        """
    )

    # --------------------------------------------------------
    # Dimension: Location
    # --------------------------------------------------------

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
        {GOLD_SCHEMA}.dim_location
        (
            location_id INTEGER PRIMARY KEY,
            city VARCHAR(150) NOT NULL,
            district VARCHAR(150),
            state VARCHAR(150),
            country VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            timezone VARCHAR(100)
        );
        """
    )

    # --------------------------------------------------------
    # Dimension: Date
    # --------------------------------------------------------

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
        {GOLD_SCHEMA}.dim_date
        (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL UNIQUE,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name VARCHAR(20) NOT NULL,
            day INTEGER NOT NULL,
            day_name VARCHAR(20) NOT NULL,
            day_of_week INTEGER NOT NULL,
            week_of_year INTEGER NOT NULL
        );
        """
    )

    # --------------------------------------------------------
    # Dimension: Time
    # --------------------------------------------------------

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
        {GOLD_SCHEMA}.dim_time
        (
            time_key INTEGER PRIMARY KEY,
            hour INTEGER NOT NULL,
            minute INTEGER NOT NULL,
            hour_12 INTEGER NOT NULL,
            am_pm VARCHAR(2) NOT NULL,
            time_label VARCHAR(10) NOT NULL,
            day_period VARCHAR(20) NOT NULL
        );
        """
    )

    # --------------------------------------------------------
    # Fact: Weather
    # --------------------------------------------------------

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
        {GOLD_SCHEMA}.fact_weather_hourly
        (
            weather_id BIGSERIAL PRIMARY KEY,

            location_id INTEGER NOT NULL,

            date_key INTEGER NOT NULL,

            time_key INTEGER NOT NULL,

            timestamp TIMESTAMP NOT NULL,

            data_granularity VARCHAR(20) NOT NULL DEFAULT 'historical',

            temperature_2m DOUBLE PRECISION,

            relative_humidity_2m DOUBLE PRECISION,

            dew_point_2m DOUBLE PRECISION,

            apparent_temperature DOUBLE PRECISION,

            precipitation DOUBLE PRECISION,

            rain DOUBLE PRECISION,

            snowfall DOUBLE PRECISION,

            weather_code INTEGER,

            cloud_cover DOUBLE PRECISION,

            surface_pressure DOUBLE PRECISION,

            wind_speed_10m DOUBLE PRECISION,

            wind_direction_10m DOUBLE PRECISION,

            wind_gusts_10m DOUBLE PRECISION,

            CONSTRAINT fk_weather_location
                FOREIGN KEY (location_id)
                REFERENCES {GOLD_SCHEMA}.dim_location(location_id),

            CONSTRAINT fk_weather_date
                FOREIGN KEY (date_key)
                REFERENCES {GOLD_SCHEMA}.dim_date(date_key),

            CONSTRAINT fk_weather_time
                FOREIGN KEY (time_key)
                REFERENCES {GOLD_SCHEMA}.dim_time(time_key),

            CONSTRAINT uq_weather_location_timestamp
                UNIQUE (location_id, timestamp)
        );
        """
    )

    # Additive migration for an existing Gold table.
    cursor.execute(
        f"""
        ALTER TABLE {GOLD_SCHEMA}.fact_weather_hourly
        ADD COLUMN IF NOT EXISTS data_granularity VARCHAR(20)
        NOT NULL DEFAULT 'historical';
        """
    )

    # --------------------------------------------------------
    # Indexes
    # --------------------------------------------------------

    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS
        idx_fact_weather_location
        ON {GOLD_SCHEMA}.fact_weather_hourly(location_id);
        """
    )

    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS
        idx_fact_weather_date
        ON {GOLD_SCHEMA}.fact_weather_hourly(date_key);
        """
    )

    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS
        idx_fact_weather_timestamp
        ON {GOLD_SCHEMA}.fact_weather_hourly(timestamp);
        """
    )

    connection.commit()

    cursor.close()

    logger.info(
        "Gold schema created successfully."
    )


def load_dim_location(
    connection,
    cities
):
    """Load location dimension."""

    cursor = connection.cursor()

    rows = []

    for _, row in cities.iterrows():

        rows.append(
            (
                int(row["location_id"]),
                clean_text(row["city"]),
                clean_text(row["district"]),
                clean_text(row["state"]),
                clean_text(row["country"]),
                float(row["latitude"]),
                float(row["longitude"]),
                clean_text(row["timezone"]),
            )
        )

    sql = f"""
        INSERT INTO
        {GOLD_SCHEMA}.dim_location
        (
            location_id,
            city,
            district,
            state,
            country,
            latitude,
            longitude,
            timezone
        )
        VALUES %s

        ON CONFLICT (location_id)
        DO UPDATE SET
            city = EXCLUDED.city,
            district = EXCLUDED.district,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            timezone = EXCLUDED.timezone;
    """

    execute_values(
        cursor,
        sql,
        rows,
        page_size=100,
    )

    connection.commit()

    cursor.close()

    logger.info(
        "dim_location loaded | Rows=%s",
        len(rows),
    )


def generate_date_dimension(
    silver_files
):
    """Generate unique date dimension from Silver data."""

    dates = set()

    for file in silver_files:

        df = pd.read_parquet(
            file,
            engine="pyarrow",
        )

        timestamps = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
        ).dropna()

        for value in timestamps:

            dates.add(
                value.date()
            )

    rows = []

    for date_value in sorted(dates):

        timestamp = pd.Timestamp(
            date_value
        )

        date_key = int(
            timestamp.strftime("%Y%m%d")
        )

        rows.append(
            (
                date_key,
                date_value,
                timestamp.year,
                timestamp.quarter,
                timestamp.month,
                timestamp.strftime("%B"),
                timestamp.day,
                timestamp.strftime("%A"),
                timestamp.dayofweek + 1,
                int(timestamp.isocalendar().week),
            )
        )

    return rows


def load_dim_date(
    connection,
    silver_files
):
    """Load date dimension."""

    rows = generate_date_dimension(
        silver_files
    )

    if not rows:
        return

    cursor = connection.cursor()

    sql = f"""
        INSERT INTO
        {GOLD_SCHEMA}.dim_date
        (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            day,
            day_name,
            day_of_week,
            week_of_year
        )
        VALUES %s

        ON CONFLICT (date_key)
        DO UPDATE SET
            full_date = EXCLUDED.full_date,
            year = EXCLUDED.year,
            quarter = EXCLUDED.quarter,
            month = EXCLUDED.month,
            month_name = EXCLUDED.month_name,
            day = EXCLUDED.day,
            day_name = EXCLUDED.day_name,
            day_of_week = EXCLUDED.day_of_week,
            week_of_year = EXCLUDED.week_of_year;
    """

    execute_values(
        cursor,
        sql,
        rows,
        page_size=1000,
    )

    connection.commit()

    cursor.close()

    logger.info(
        "dim_date loaded | Rows=%s",
        len(rows),
    )


def get_day_period(hour):
    """Classify hour into day period."""

    if 5 <= hour < 12:
        return "Morning"

    if 12 <= hour < 17:
        return "Afternoon"

    if 17 <= hour < 21:
        return "Evening"

    return "Night"


def load_dim_time(connection):
    """Load 24-hour time dimension."""

    rows = []

    for hour in range(24):

        if hour == 0:
            hour_12 = 12
            am_pm = "AM"

        elif hour < 12:
            hour_12 = hour
            am_pm = "AM"

        elif hour == 12:
            hour_12 = 12
            am_pm = "PM"

        else:
            hour_12 = hour - 12
            am_pm = "PM"

        rows.append(
            (
                hour,
                hour,
                0,
                hour_12,
                am_pm,
                f"{hour_12}:00 {am_pm}",
                get_day_period(hour),
            )
        )

    cursor = connection.cursor()

    sql = f"""
        INSERT INTO
        {GOLD_SCHEMA}.dim_time
        (
            time_key,
            hour,
            minute,
            hour_12,
            am_pm,
            time_label,
            day_period
        )
        VALUES %s

        ON CONFLICT (time_key)
        DO UPDATE SET
            hour = EXCLUDED.hour,
            minute = EXCLUDED.minute,
            hour_12 = EXCLUDED.hour_12,
            am_pm = EXCLUDED.am_pm,
            time_label = EXCLUDED.time_label,
            day_period = EXCLUDED.day_period;
    """

    execute_values(
        cursor,
        sql,
        rows,
        page_size=24,
    )

    connection.commit()

    cursor.close()

    logger.info(
        "dim_time loaded | Rows=%s",
        len(rows),
    )


def load_fact_weather(
    connection,
    silver_files
):
    """Load weather fact table from Silver Parquet files."""

    cursor = connection.cursor()

    total_rows = 0

    for file in silver_files:

        logger.info(
            "Loading Silver file into Gold: %s",
            file,
        )

        df = pd.read_parquet(
            file,
            engine="pyarrow",
        )

        if df.empty:
            continue

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
        )

        df = df.dropna(
            subset=[
                "timestamp",
                "location_id",
            ]
        )

        rows = []

        for _, row in df.iterrows():

            timestamp = row["timestamp"]

            date_key = int(
                timestamp.strftime("%Y%m%d")
            )

            time_key = int(
                timestamp.hour
            )

            def safe_value(column):

                value = row.get(
                    column
                )

                if pd.isna(value):
                    return None

                return value

            weather_code = safe_value(
                "weather_code"
            )

            if weather_code is not None:
                weather_code = int(
                    weather_code
                )

            rows.append(
                (
                    int(row["location_id"]),
                    date_key,
                    time_key,
                    timestamp.to_pydatetime(),

                    safe_value(
                        "temperature_2m"
                    ),

                    safe_value(
                        "relative_humidity_2m"
                    ),

                    safe_value(
                        "dew_point_2m"
                    ),

                    safe_value(
                        "apparent_temperature"
                    ),

                    safe_value(
                        "precipitation"
                    ),

                    safe_value(
                        "rain"
                    ),

                    safe_value(
                        "snowfall"
                    ),

                    weather_code,

                    safe_value(
                        "cloud_cover"
                    ),

                    safe_value(
                        "surface_pressure"
                    ),

                    safe_value(
                        "wind_speed_10m"
                    ),

                    safe_value(
                        "wind_direction_10m"
                    ),

                    safe_value(
                        "wind_gusts_10m"
                    ),
                )
            )

        sql = f"""
            INSERT INTO
            {GOLD_SCHEMA}.fact_weather_hourly
            (
                location_id,
                date_key,
                time_key,
                timestamp,
                temperature_2m,
                relative_humidity_2m,
                dew_point_2m,
                apparent_temperature,
                precipitation,
                rain,
                snowfall,
                weather_code,
                cloud_cover,
                surface_pressure,
                wind_speed_10m,
                wind_direction_10m,
                wind_gusts_10m
            )
            VALUES %s

            ON CONFLICT
            (location_id, timestamp)

            DO UPDATE SET

                date_key =
                    EXCLUDED.date_key,

                time_key =
                    EXCLUDED.time_key,

                temperature_2m =
                    EXCLUDED.temperature_2m,

                relative_humidity_2m =
                    EXCLUDED.relative_humidity_2m,

                dew_point_2m =
                    EXCLUDED.dew_point_2m,

                apparent_temperature =
                    EXCLUDED.apparent_temperature,

                precipitation =
                    EXCLUDED.precipitation,

                rain =
                    EXCLUDED.rain,

                snowfall =
                    EXCLUDED.snowfall,

                weather_code =
                    EXCLUDED.weather_code,

                cloud_cover =
                    EXCLUDED.cloud_cover,

                surface_pressure =
                    EXCLUDED.surface_pressure,

                wind_speed_10m =
                    EXCLUDED.wind_speed_10m,

                wind_direction_10m =
                    EXCLUDED.wind_direction_10m,

                wind_gusts_10m =
                    EXCLUDED.wind_gusts_10m;
        """

        execute_values(
            cursor,
            sql,
            rows,
            page_size=5000,
        )

        connection.commit()

        total_rows += len(rows)

        logger.info(
            "Fact rows processed: %s",
            len(rows),
        )

    cursor.close()

    logger.info(
        "fact_weather_hourly loaded | "
        "Total rows=%s",
        total_rows,
    )


def validate_gold(connection):
    """Perform final Gold data quality checks."""

    cursor = connection.cursor()

    # --------------------------------------------------------
    # Location duplicate check
    # --------------------------------------------------------

    cursor.execute(
        f"""
        SELECT
            location_id,
            COUNT(*)
        FROM
            {GOLD_SCHEMA}.dim_location
        GROUP BY
            location_id
        HAVING COUNT(*) > 1;
        """
    )

    duplicate_locations = cursor.fetchall()

    if duplicate_locations:

        raise ValueError(
            f"Duplicate location_id values found "
            f"in dim_location: {duplicate_locations}"
        )

    # --------------------------------------------------------
    # Fact count
    # --------------------------------------------------------

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM
            {GOLD_SCHEMA}.fact_weather_hourly;
        """
    )

    fact_count = cursor.fetchone()[0]

    # --------------------------------------------------------
    # Location count
    # --------------------------------------------------------

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM
            {GOLD_SCHEMA}.dim_location;
        """
    )

    location_count = cursor.fetchone()[0]

    # --------------------------------------------------------
    # Date count
    # --------------------------------------------------------

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM
            {GOLD_SCHEMA}.dim_date;
        """
    )

    date_count = cursor.fetchone()[0]

    # --------------------------------------------------------
    # Time count
    # --------------------------------------------------------

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM
            {GOLD_SCHEMA}.dim_time;
        """
    )

    time_count = cursor.fetchone()[0]

    cursor.close()

    logger.info(
        "Gold validation results:"
    )

    logger.info(
        "dim_location rows = %s",
        location_count,
    )

    logger.info(
        "dim_date rows = %s",
        date_count,
    )

    logger.info(
        "dim_time rows = %s",
        time_count,
    )

    logger.info(
        "fact_weather_hourly rows = %s",
        fact_count,
    )

    if location_count == 0:
        raise ValueError(
            "Gold validation failed: "
            "dim_location is empty."
        )

    if time_count != 24:
        raise ValueError(
            f"Gold validation failed: "
            f"Expected 24 time rows, got {time_count}"
        )

    if fact_count == 0:
        raise ValueError(
            "Gold validation failed: "
            "fact_weather_hourly is empty."
        )

    logger.info(
        "Gold data quality validation: PASSED"
    )


def gold_layer_task():
    """
    GoldLayer

    Responsibilities:
    - Connect to PostgreSQL
    - Create Gold schema
    - Create star schema
    - Load dim_location
    - Load dim_date
    - Load dim_time
    - Load fact_weather_hourly
    - Validate Gold data
    """

    logger.info("=" * 70)
    logger.info("WEATHERLAKE GOLDLAYER")
    logger.info("=" * 70)

    ensure_directories()

    silver_files = list(
        SILVER_DIR.rglob(
            "weather_cleaned.parquet"
        )
    )

    if not silver_files:

        raise FileNotFoundError(
            "No Silver files found."
        )

    logger.info(
        "Silver files found: %s",
        len(silver_files),
    )

    cities = load_cities()

    connection = None

    try:

        connection = get_postgres_connection()

        logger.info(
            "Connected to PostgreSQL."
        )

        create_gold_schema(
            connection
        )

        load_dim_location(
            connection,
            cities,
        )

        load_dim_date(
            connection,
            silver_files,
        )

        load_dim_time(
            connection
        )

        load_fact_weather(
            connection,
            silver_files,
        )

        validate_gold(
            connection
        )

        logger.info(
            "GOLD LAYER COMPLETED SUCCESSFULLY"
        )

    except Exception:

        if connection:
            connection.rollback()

        logger.exception(
            "GoldLayer failed."
        )

        raise

    finally:

        if connection:
            connection.close()

            logger.info(
                "PostgreSQL connection closed."
            )


# ============================================================
# AIRFLOW DAG
# ============================================================

default_args = {
    "owner": "WeatherLake",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id=DAG_ID,

    description=(
        "WeatherLake Medallion ETL: "
        "Bronze -> Silver -> Gold"
    ),

    start_date=datetime(
        2026,
        1,
        1
    ),

    schedule="0 2 * * *",

    catchup=False,

    default_args=default_args,

    tags=[
        "WeatherLake",
        "ETL",
        "Bronze",
        "Silver",
        "Gold",
        "Weather",
    ],

) as dag:

    bronze_layer = PythonOperator(
        task_id="BronzeLayer",

        python_callable=bronze_layer_task,
    )

    silver_layer = PythonOperator(
        task_id="SilverLayer",

        python_callable=silver_layer_task,
    )

    gold_layer = PythonOperator(
        task_id="GoldLayer",

        python_callable=gold_layer_task,
    )

    # ========================================================
    # ETL DEPENDENCY
    # ========================================================

    bronze_layer >> silver_layer >> gold_layer