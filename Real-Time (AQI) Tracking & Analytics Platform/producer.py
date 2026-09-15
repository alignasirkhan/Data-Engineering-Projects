import json
import os
import uuid
import time
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData


# ============================================================
# Configuration
# ============================================================

load_dotenv()

EVENT_HUB_CONNECTION_STRING = os.getenv(
    "EVENT_HUB_CONNECTION_STRING"
)

EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

if not EVENT_HUB_CONNECTION_STRING:
    raise ValueError(
        "EVENT_HUB_CONNECTION_STRING is not set in .env"
    )

if not EVENT_HUB_NAME:
    raise ValueError(
        "EVENT_HUB_NAME is not set in .env"
    )


OPEN_METEO_URL = (
    "https://air-quality-api.open-meteo.com/v1/air-quality"
)

EXTRACTION_INTERVAL_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# Production Cities
# ============================================================

CITIES = [
    {
        "city": "Delhi",
        "latitude": 28.6139,
        "longitude": 77.2090,
    },
    {
        "city": "Noida",
        "latitude": 28.5355,
        "longitude": 77.3910,
    },
    {
        "city": "Gurugram",
        "latitude": 28.4595,
        "longitude": 77.0266,
    },
    {
        "city": "Lucknow",
        "latitude": 26.8467,
        "longitude": 80.9462,
    },
    {
        "city": "Agra",
        "latitude": 27.1767,
        "longitude": 78.0081,
    },
    {
        "city": "Jaipur",
        "latitude": 26.9124,
        "longitude": 75.7873,
    },
    {
        "city": "Mumbai",
        "latitude": 19.0760,
        "longitude": 72.8777,
    },
    {
        "city": "Hyderabad",
        "latitude": 17.3850,
        "longitude": 78.4867,
    },
    {
        "city": "Bengaluru",
        "latitude": 12.9716,
        "longitude": 77.5946,
    },
    {
        "city": "Aligarh",
        "latitude": 27.8974,
        "longitude": 78.0880,
    },
]


# ============================================================
# Open-Meteo API Parameters
# ============================================================

CURRENT_PARAMETERS = (
    "us_aqi,"
    "pm2_5,"
    "pm10,"
    "carbon_monoxide,"
    "nitrogen_dioxide,"
    "sulphur_dioxide,"
    "ozone"
)


# ============================================================
# Fetch AQI Data
# ============================================================

def fetch_aqi_data():

    latitudes = ",".join(
        str(city["latitude"])
        for city in CITIES
    )

    longitudes = ",".join(
        str(city["longitude"])
        for city in CITIES
    )

    parameters = {
        "latitude": latitudes,
        "longitude": longitudes,
        "current": CURRENT_PARAMETERS,
        "timezone": "UTC"
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = requests.get(
                OPEN_METEO_URL,
                params=parameters,
                timeout=REQUEST_TIMEOUT_SECONDS
            )

            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                data = [data]

            if len(data) != len(CITIES):
                raise ValueError(
                    f"Expected {len(CITIES)} locations, "
                    f"received {len(data)}"
                )

            return data

        except Exception as exc:

            last_error = exc

            logger.warning(
                f"Open-Meteo request failed "
                f"(attempt {attempt}/{MAX_RETRIES}): {exc}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(5)

    raise RuntimeError(
        f"Unable to fetch AQI data after "
        f"{MAX_RETRIES} attempts: {last_error}"
    )


# ============================================================
# Create Event
# ============================================================

def create_event(city_info, data):

    current = data.get("current", {})

    required_values = [
        current.get("us_aqi"),
        current.get("pm2_5"),
        current.get("pm10"),
        current.get("carbon_monoxide"),
        current.get("nitrogen_dioxide"),
        current.get("sulphur_dioxide"),
        current.get("ozone"),
    ]

    if any(value is None for value in required_values):
        return None

    now_utc = datetime.now(timezone.utc)

    now_ist = now_utc.astimezone(
        ZoneInfo("Asia/Kolkata")
    )

    event = {
        "event_id": str(uuid.uuid4()),

        "city": city_info["city"],

        "latitude": city_info["latitude"],
        "longitude": city_info["longitude"],

        "timestamp_utc": now_utc.isoformat(),

        "timestamp_ist": now_ist.isoformat(),

        "api_timestamp_utc": current["time"],

        "us_aqi": current["us_aqi"],
        "pm2_5": current["pm2_5"],
        "pm10": current["pm10"],
        "carbon_monoxide": current["carbon_monoxide"],
        "nitrogen_dioxide": current["nitrogen_dioxide"],
        "sulphur_dioxide": current["sulphur_dioxide"],
        "ozone": current["ozone"],

        "source": "open-meteo"
    }

    return event


# ============================================================
# Send Events to Azure Event Hubs
# ============================================================

def send_to_event_hub(events):

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME
    )

    try:

        event_data_batch = producer.create_batch()

        events_sent = 0

        for event in events:

            event_data = EventData(
                json.dumps(event)
            )

            try:

                event_data_batch.add(event_data)
                events_sent += 1

            except ValueError:

                producer.send_batch(event_data_batch)

                event_data_batch = producer.create_batch()

                event_data_batch.add(event_data)

                events_sent += 1

        if len(event_data_batch) > 0:
            producer.send_batch(event_data_batch)

        return events_sent

    finally:

        producer.close()


# ============================================================
# Production Extraction Cycle
# ============================================================

def run_extraction_cycle():

    logger.info(
        "Fetching AQI data for %d cities...",
        len(CITIES)
    )

    api_results = fetch_aqi_data()

    events = []

    for city_info, city_data in zip(
        CITIES,
        api_results
    ):

        try:

            event = create_event(
                city_info,
                city_data
            )

            if event is None:

                logger.warning(
                    "%s skipped: incomplete AQI data",
                    city_info["city"]
                )

                continue

            events.append(event)

        except Exception as exc:

            logger.exception(
                "%s failed: %s",
                city_info["city"],
                exc
            )

    if not events:

        logger.warning(
            "No valid AQI events generated."
        )

        return

    events_sent = send_to_event_hub(events)

    logger.info(
        "Successfully sent %d/%d AQI events to Event Hubs",
        events_sent,
        len(CITIES)
    )

    for event in events:

        logger.info(
            "%s | AQI=%s | PM2.5=%s | PM10=%s",
            event["city"],
            event["us_aqi"],
            event["pm2_5"],
            event["pm10"]
        )


# ============================================================
# Main Production Loop
# ============================================================

def main():

    logger.info("=" * 70)
    logger.info(
        "REAL-TIME AQI PRODUCTION PRODUCER STARTED"
    )
    logger.info("=" * 70)

    logger.info(
        "Cities configured: %d",
        len(CITIES)
    )

    logger.info(
        "Extraction interval: %d seconds",
        EXTRACTION_INTERVAL_SECONDS
    )

    logger.info(
        "Source: Open-Meteo Air Quality API"
    )

    logger.info(
        "Destination: Azure Event Hubs"
    )

    logger.info("=" * 70)

    while True:

        cycle_start = time.time()

        try:

            run_extraction_cycle()

        except Exception as exc:

            logger.exception(
                "Production extraction cycle failed: %s",
                exc
            )

        elapsed = time.time() - cycle_start

        sleep_seconds = max(
            0,
            EXTRACTION_INTERVAL_SECONDS - elapsed
        )

        logger.info(
            "Next extraction in %.1f seconds",
            sleep_seconds
        )

        time.sleep(sleep_seconds)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()