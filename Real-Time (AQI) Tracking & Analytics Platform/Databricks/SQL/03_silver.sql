-- ============================================================
-- 03_silver.sql
-- Real-Time AQI Tracking & Analytics Platform
-- ============================================================

USE CATALOG aqi_platform;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.aqi_events
(
    event_id              STRING,
    city                  STRING,
    latitude              DOUBLE,
    longitude             DOUBLE,

    timestamp_utc         TIMESTAMP,
    timestamp_ist         TIMESTAMP,
    api_timestamp_utc     TIMESTAMP,

    us_aqi                DOUBLE,
    aqi_category          STRING,

    pm2_5                  DOUBLE,
    pm10                   DOUBLE,
    carbon_monoxide       DOUBLE,
    nitrogen_dioxide       DOUBLE,
    sulphur_dioxide        DOUBLE,
    ozone                  DOUBLE,

    source                 STRING,

    ingestion_timestamp    TIMESTAMP
)
USING DELTA
COMMENT 'Silver layer containing validated and transformed AQI events';