-- ============================================================
-- 01_setup.sql
-- Real-Time AQI Tracking & Analytics Platform
-- ============================================================

-- ============================================================
-- 1. Create Catalog
-- ============================================================

CREATE CATALOG IF NOT EXISTS aqi_platform
MANAGED LOCATION
'abfss://unity-catalog-storage@dbstorageunui6cblaxhf2.dfs.core.windows.net/7405614174377389/aqi_platform';


-- ============================================================
-- 2. Use AQI Platform Catalog
-- ============================================================

USE CATALOG aqi_platform;


-- ============================================================
-- 3. Create Data Layers
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;


-- ============================================================
-- 4. Bronze Streaming Checkpoint Volume
-- ============================================================

CREATE VOLUME IF NOT EXISTS
bronze.streaming_checkpoints;


-- ============================================================
-- 5. Silver Streaming Checkpoint Volume
-- ============================================================

CREATE VOLUME IF NOT EXISTS
silver.streaming_checkpoints;


-- ============================================================
-- 6. Gold Streaming Checkpoint Volume
-- ============================================================

CREATE VOLUME IF NOT EXISTS
gold.streaming_checkpoints;