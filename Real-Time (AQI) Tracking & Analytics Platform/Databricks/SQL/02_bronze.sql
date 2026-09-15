-- ============================================================
-- AQI Platform - Bronze Layer
-- File: 02_bronze.sql
-- Purpose: Create the raw AQI events Bronze table
-- ============================================================

USE CATALOG aqi_platform;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.aqi_events
(
    event_id              STRING,
    city                  STRING,
    latitude              DOUBLE,
    longitude             DOUBLE,

    timestamp_utc         TIMESTAMP,
    timestamp_ist         TIMESTAMP,
    api_timestamp_utc     TIMESTAMP,

    us_aqi                DOUBLE,
    pm2_5                  DOUBLE,
    pm10                   DOUBLE,
    carbon_monoxide       DOUBLE,
    nitrogen_dioxide      DOUBLE,
    sulphur_dioxide       DOUBLE,
    ozone                 DOUBLE,

    source                 STRING,

    ingestion_timestamp    TIMESTAMP
)
USING DELTA
COMMENT 'Bronze layer containing raw AQI events ingested from Azure Event Hubs';


DESCRIBE TABLE aqi_platform.bronze.aqi_events;

SELECT *
FROM aqi_platform.bronze.aqi_events
LIMIT 10;