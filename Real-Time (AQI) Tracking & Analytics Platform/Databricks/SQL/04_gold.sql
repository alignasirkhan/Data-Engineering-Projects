-- ============================================================
-- 04_gold.sql
-- Real-Time AQI Tracking & Analytics Platform
-- ============================================================

USE CATALOG aqi_platform;

CREATE SCHEMA IF NOT EXISTS gold;


-- ============================================================
-- Dimension: Location
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_location
(
    location_key    BIGINT,
    city            STRING,
    latitude        DOUBLE,
    longitude       DOUBLE
)
USING DELTA
COMMENT 'Gold location dimension for AQI analytics';


-- ============================================================
-- Dimension: Date
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_date
(
    date_key        INT,
    date            DATE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      STRING,
    day             INT,
    day_of_week     INT,
    day_name        STRING
)
USING DELTA
COMMENT 'Gold calendar dimension for AQI analytics';


-- ============================================================
-- Dimension: Source
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_source
(
    source_key      INT,
    source          STRING
)
USING DELTA
COMMENT 'Gold source dimension for AQI data lineage';


-- ============================================================
-- Fact: Air Quality
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_air_quality
(
    event_id              STRING,
    location_key          BIGINT,
    date_key              INT,
    source_key            INT,

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

    ingestion_timestamp    TIMESTAMP
)
USING DELTA
COMMENT 'Gold AQI fact table for analytics and dashboard consumption';