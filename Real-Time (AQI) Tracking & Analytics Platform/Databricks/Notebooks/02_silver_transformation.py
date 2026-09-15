# Databricks notebook source
# ============================================================
# 02_silver_transformation.py
# Real-Time Air Quality Index (AQI) Tracking & Analytics Platform
#
# Bronze Delta → Silver Delta
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# ============================================================
# 1. Spark Configuration
# ============================================================

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC"
)

print("✅ Silver transformation configuration initialized")


# ============================================================
# 2. Read Bronze as a Streaming Source
# ============================================================

bronze_stream = (
    spark.readStream
    .table("aqi_platform.bronze.aqi_events")
)

print("✅ Bronze streaming source initialized")


# ============================================================
# 3. Validate Incoming Records
# ============================================================

silver_valid_stream = (
    bronze_stream

    .filter(F.col("event_id").isNotNull())

    .filter(F.col("city").isNotNull())

    .filter(F.col("timestamp_utc").isNotNull())

    .filter(F.col("us_aqi").isNotNull())
    .filter(F.col("us_aqi") >= 0)

    .filter(F.col("pm2_5").isNotNull())
    .filter(F.col("pm2_5") >= 0)

    .filter(F.col("pm10").isNotNull())
    .filter(F.col("pm10") >= 0)
)

print("✅ Silver validation rules configured")


# ============================================================
# 4. Derive AQI Category
# ============================================================

silver_transformed_stream = (
    silver_valid_stream

    .withColumn(
        "aqi_category",
        F.when(
            F.col("us_aqi") <= 50,
            "Good"
        )
        .when(
            F.col("us_aqi") <= 100,
            "Moderate"
        )
        .when(
            F.col("us_aqi") <= 150,
            "Unhealthy for Sensitive Groups"
        )
        .when(
            F.col("us_aqi") <= 200,
            "Unhealthy"
        )
        .when(
            F.col("us_aqi") <= 300,
            "Very Unhealthy"
        )
        .otherwise(
            "Hazardous"
        )
    )
)

print("✅ AQI category transformation configured")


# ============================================================
# 5. Handle Duplicate Events
# ============================================================
#
# Watermark bounds the streaming state.
# event_id is the unique business identifier.
#

silver_deduplicated_stream = (
    silver_transformed_stream

    .withWatermark(
        "timestamp_utc",
        "24 hours"
    )

    .dropDuplicates(
        ["event_id"]
    )
)

print("✅ Streaming deduplication configured")


# ============================================================
# 6. Select Final Silver Schema
# ============================================================

silver_final_stream = silver_deduplicated_stream.select(
    "event_id",
    "city",
    "latitude",
    "longitude",

    "timestamp_utc",
    "timestamp_ist",
    "api_timestamp_utc",

    "us_aqi",
    "aqi_category",

    "pm2_5",
    "pm10",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",

    "source",

    "ingestion_timestamp"
)

print("✅ Silver schema configured")


# ============================================================
# 7. Merge Each Micro-Batch into Silver
# ============================================================

def merge_to_silver(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    silver_table = DeltaTable.forName(
        spark,
        "aqi_platform.silver.aqi_events"
    )

    (
        silver_table.alias("target")

        .merge(
            batch_df.alias("source"),
            "target.event_id = source.event_id"
        )

        .whenNotMatchedInsertAll()

        .execute()
    )

    print(
        f"✅ Silver micro-batch {batch_id} processed: "
        f"{batch_df.count():,} records"
    )


# ============================================================
# 8. Silver Streaming Checkpoint
# ============================================================

silver_checkpoint = (
    "/Volumes/aqi_platform/silver/streaming_checkpoints/"
    "silver_transformation"
)


# ============================================================
# 9. Start Silver AvailableNow Pipeline
# ============================================================

silver_query = (
    silver_final_stream
    .writeStream
    .foreachBatch(
        merge_to_silver
    )
    .outputMode("append")
    .option(
        "checkpointLocation",
        silver_checkpoint
    )
    .trigger(
        availableNow=True
    )
    .start()
)

print("✅ Silver AvailableNow pipeline started")


# ============================================================
# 10. Wait for Silver Pipeline to Complete
# ============================================================

silver_query.awaitTermination()

print("✅ Silver AvailableNow pipeline completed")