-- ============================================================
-- 05_validation.sql
-- Real-Time AQI Tracking & Analytics Platform
-- ============================================================

USE CATALOG aqi_platform;

-- ============================================================
-- 1. Layer Record Counts
-- ============================================================

SELECT
    'Bronze' AS layer,
    COUNT(*) AS record_count
FROM bronze.aqi_events

UNION ALL

SELECT
    'Silver' AS layer,
    COUNT(*) AS record_count
FROM silver.aqi_events

UNION ALL

SELECT
    'Gold Fact' AS layer,
    COUNT(*) AS record_count
FROM gold.fact_air_quality;


-- ============================================================
-- 2. Gold Dimension Counts
-- ============================================================

SELECT
    'dim_location' AS table_name,
    COUNT(*) AS record_count
FROM gold.dim_location

UNION ALL

SELECT
    'dim_date' AS table_name,
    COUNT(*) AS record_count
FROM gold.dim_date

UNION ALL

SELECT
    'dim_source' AS table_name,
    COUNT(*) AS record_count
FROM gold.dim_source;


-- ============================================================
-- 3. AQI Category Distribution
-- ============================================================

SELECT
    aqi_category,
    COUNT(*) AS record_count,
    ROUND(AVG(us_aqi), 2) AS avg_aqi,
    ROUND(AVG(pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(pm10), 2) AS avg_pm10
FROM gold.fact_air_quality
GROUP BY aqi_category
ORDER BY record_count DESC;


-- ============================================================
-- 4. Null Validation
-- ============================================================

SELECT
    COUNT(*) AS total_records,

    SUM(
        CASE WHEN event_id IS NULL THEN 1 ELSE 0 END
    ) AS null_event_id,

    SUM(
        CASE WHEN location_key IS NULL THEN 1 ELSE 0 END
    ) AS null_location_key,

    SUM(
        CASE WHEN date_key IS NULL THEN 1 ELSE 0 END
    ) AS null_date_key,

    SUM(
        CASE WHEN source_key IS NULL THEN 1 ELSE 0 END
    ) AS null_source_key,

    SUM(
        CASE WHEN us_aqi IS NULL THEN 1 ELSE 0 END
    ) AS null_us_aqi

FROM gold.fact_air_quality;


-- ============================================================
-- 5. Duplicate Event Validation
-- ============================================================

SELECT
    event_id,
    COUNT(*) AS duplicate_count
FROM gold.fact_air_quality
GROUP BY event_id
HAVING COUNT(*) > 1;


-- ============================================================
-- 6. Foreign-Key Integrity — Location
-- ============================================================

SELECT
    COUNT(*) AS orphan_location_records
FROM gold.fact_air_quality f
LEFT JOIN gold.dim_location l
    ON f.location_key = l.location_key
WHERE l.location_key IS NULL;


-- ============================================================
-- 7. Foreign-Key Integrity — Date
-- ============================================================

SELECT
    COUNT(*) AS orphan_date_records
FROM gold.fact_air_quality f
LEFT JOIN gold.dim_date d
    ON f.date_key = d.date_key
WHERE d.date_key IS NULL;


-- ============================================================
-- 8. Foreign-Key Integrity — Source
-- ============================================================

SELECT
    COUNT(*) AS orphan_source_records
FROM gold.fact_air_quality f
LEFT JOIN gold.dim_source s
    ON f.source_key = s.source_key
WHERE s.source_key IS NULL;


-- ============================================================
-- 9. Latest AQI Readings
-- ============================================================

SELECT
    f.event_id,
    l.city,
    f.timestamp_utc,
    f.timestamp_ist,
    f.us_aqi,
    f.aqi_category,
    f.pm2_5,
    f.pm10,
    s.source
FROM gold.fact_air_quality f
JOIN gold.dim_location l
    ON f.location_key = l.location_key
JOIN gold.dim_source s
    ON f.source_key = s.source_key
ORDER BY f.timestamp_utc DESC
LIMIT 10;


-- ============================================================
-- 10. Location-Level AQI Summary
-- ============================================================

SELECT
    l.city,
    COUNT(*) AS readings,
    ROUND(AVG(f.us_aqi), 2) AS avg_aqi,
    ROUND(MAX(f.us_aqi), 2) AS max_aqi,
    ROUND(AVG(f.pm2_5), 2) AS avg_pm2_5,
    ROUND(AVG(f.pm10), 2) AS avg_pm10
FROM gold.fact_air_quality f
JOIN gold.dim_location l
    ON f.location_key = l.location_key
GROUP BY l.city
ORDER BY avg_aqi DESC;