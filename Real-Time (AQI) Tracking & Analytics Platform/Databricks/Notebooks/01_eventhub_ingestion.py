# Databricks notebook source
# ============================================================
# 01_eventhub_ingestion.py
# Real-Time Air Quality Index (AQI) Tracking & Analytics Platform
#
# Event Hubs → Bronze Delta
# ============================================================

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp
)

# ============================================================
# 1. Retrieve Event Hubs Credentials
# ============================================================

event_hub_connection_string = dbutils.secrets.get(
    scope="aqi-platform",
    key="eventhub-consumer-connection-string"
)

print("✅ Event Hubs secret retrieved successfully")


# ============================================================
# 2. Configure Event Hubs Kafka Connection
# ============================================================

kafka_options = {
    "kafka.bootstrap.servers":
        "aqi-platform-eh.servicebus.windows.net:9093",

    "subscribe": "aqi-events",

    "kafka.security.protocol":
        "SASL_SSL",

    "kafka.sasl.mechanism":
        "PLAIN",

    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" '
        f'password="{event_hub_connection_string}";'
    ),

    "failOnDataLoss": "false"
}

print("✅ Event Hubs Kafka configuration created")


# ============================================================
# 3. Define Incoming AQI Event Schema
# ============================================================

aqi_event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),

    StructField("timestamp_utc", StringType(), True),
    StructField("timestamp_ist", StringType(), True),
    StructField("api_timestamp_utc", StringType(), True),

    StructField("us_aqi", DoubleType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("carbon_monoxide", DoubleType(), True),
    StructField("nitrogen_dioxide", DoubleType(), True),
    StructField("sulphur_dioxide", DoubleType(), True),
    StructField("ozone", DoubleType(), True),

    StructField("source", StringType(), True)
])

print("✅ AQI event schema defined")


# ============================================================
# 4. Read AQI Events from Azure Event Hubs
# ============================================================

raw_events = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .option("startingOffsets", "latest")
    .load()
)

print("✅ Event Hubs streaming source initialized")


# ============================================================
# 5. Parse Incoming JSON Events
# ============================================================

parsed_events = (
    raw_events
    .select(
        col("value").cast("string").alias("json_value")
    )
    .select(
        from_json(
            col("json_value"),
            aqi_event_schema
        ).alias("event")
    )
    .select("event.*")
)

print("✅ AQI JSON parsing configured")


# ============================================================
# 6. Prepare Bronze Records
# ============================================================

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)

bronze_events = (
    parsed_events

    .withColumn(
        "timestamp_utc",
        to_timestamp(col("timestamp_utc"))
    )

    .withColumn(
        "timestamp_ist",
        to_timestamp(col("timestamp_ist"))
    )

    .withColumn(
        "api_timestamp_utc",
        to_timestamp(
            col("api_timestamp_utc"),
            "yyyy-MM-dd'T'HH:mm"
        )
    )

    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    .select(
        "event_id",
        "city",
        "latitude",
        "longitude",
        "timestamp_utc",
        "timestamp_ist",
        "api_timestamp_utc",
        "us_aqi",
        "pm2_5",
        "pm10",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "source",
        "ingestion_timestamp"
    )
)

print("✅ Bronze records prepared")


# ============================================================
# 7. Write Events to Bronze Delta
# ============================================================

bronze_checkpoint = (
    "/Volumes/aqi_platform/bronze/streaming_checkpoints/"
    "eventhub_ingestion"
)

bronze_query = (
    bronze_events
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        bronze_checkpoint
    )
    .toTable(
        "aqi_platform.bronze.aqi_events"
    )
)

print("✅ Bronze streaming pipeline is running")


# ============================================================
# 8. Keep Streaming Query Active
# ============================================================

bronze_query.awaitTermination()