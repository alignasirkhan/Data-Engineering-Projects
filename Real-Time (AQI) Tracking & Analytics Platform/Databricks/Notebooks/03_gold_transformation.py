# Databricks notebook source
# ============================================================
# 03_gold_transformation.py
# Real-Time Air Quality Index (AQI) Tracking & Analytics Platform
#
# Silver Delta → Gold Star Schema
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

print("✅ Gold transformation configuration initialized")


# ============================================================
# 2. Read Silver as Streaming Source
# ============================================================

silver_stream = (
    spark.readStream
    .table("aqi_platform.silver.aqi_events")
)

print("✅ Silver streaming source initialized")


# ============================================================
# 3. Gold Transformation Function
# ============================================================

def process_gold_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(
        f"Processing Gold micro-batch {batch_id}: "
        f"{batch_df.count():,} records"
    )

    # ========================================================
    # Location Dimension
    # ========================================================

    batch_locations = (
        batch_df
        .select(
            "city",
            "latitude",
            "longitude"
        )
        .dropDuplicates()
        .withColumn(
            "location_key",
            F.abs(
                F.xxhash64(
                    "city",
                    "latitude",
                    "longitude"
                )
            )
        )
        .select(
            "location_key",
            "city",
            "latitude",
            "longitude"
        )
    )

    location_table = DeltaTable.forName(
        spark,
        "aqi_platform.gold.dim_location"
    )

    (
        location_table.alias("target")
        .merge(
            batch_locations.alias("source"),
            """
            target.city = source.city
            AND target.latitude = source.latitude
            AND target.longitude = source.longitude
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("✅ Location dimension updated")


    # ========================================================
    # Date Dimension
    # ========================================================

    batch_dates = (
        batch_df
        .select(
            F.to_date(
                "timestamp_utc"
            ).alias("date")
        )
        .dropDuplicates()
        .withColumn(
            "date_key",
            F.date_format(
                "date",
                "yyyyMMdd"
            ).cast("int")
        )
        .withColumn(
            "year",
            F.year("date")
        )
        .withColumn(
            "quarter",
            F.quarter("date")
        )
        .withColumn(
            "month",
            F.month("date")
        )
        .withColumn(
            "month_name",
            F.date_format(
                "date",
                "MMMM"
            )
        )
        .withColumn(
            "day",
            F.dayofmonth("date")
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("date")
        )
        .withColumn(
            "day_name",
            F.date_format(
                "date",
                "EEEE"
            )
        )
        .select(
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name"
        )
    )

    date_table = DeltaTable.forName(
        spark,
        "aqi_platform.gold.dim_date"
    )

    (
        date_table.alias("target")
        .merge(
            batch_dates.alias("source"),
            "target.date_key = source.date_key"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("✅ Date dimension updated")


    # ========================================================
    # Source Dimension
    # ========================================================

    batch_sources = (
        batch_df
        .select("source")
        .dropDuplicates()
        .withColumn(
            "source_key",
            F.pmod(
                F.abs(
                    F.xxhash64("source")
                ),
                F.lit(2147483647)
            ).cast("int")
        )
        .select(
            "source_key",
            "source"
        )
    )

    source_table = DeltaTable.forName(
        spark,
        "aqi_platform.gold.dim_source"
    )

    (
        source_table.alias("target")
        .merge(
            batch_sources.alias("source"),
            "target.source = source.source"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("✅ Source dimension updated")


    # ========================================================
    # Reload Dimensions
    # ========================================================

    dim_location = spark.table(
        "aqi_platform.gold.dim_location"
    )

    dim_date = spark.table(
        "aqi_platform.gold.dim_date"
    )

    dim_source = spark.table(
        "aqi_platform.gold.dim_source"
    )


    # ========================================================
    # Build Fact Records
    # ========================================================

    fact_batch = (
        batch_df.alias("s")

        .join(
            dim_location.alias("l"),
            (
                (F.col("s.city") == F.col("l.city")) &
                (F.col("s.latitude") == F.col("l.latitude")) &
                (F.col("s.longitude") == F.col("l.longitude"))
            ),
            "left"
        )

        .join(
            dim_date.alias("d"),
            F.to_date(
                F.col("s.timestamp_utc")
            ) == F.col("d.date"),
            "left"
        )

        .join(
            dim_source.alias("src"),
            F.col("s.source") == F.col("src.source"),
            "left"
        )

        .select(
            F.col("s.event_id"),
            F.col("l.location_key"),
            F.col("d.date_key"),
            F.col("src.source_key"),

            F.col("s.timestamp_utc"),
            F.col("s.timestamp_ist"),
            F.col("s.api_timestamp_utc"),

            F.col("s.us_aqi"),
            F.col("s.aqi_category"),

            F.col("s.pm2_5"),
            F.col("s.pm10"),
            F.col("s.carbon_monoxide"),
            F.col("s.nitrogen_dioxide"),
            F.col("s.sulphur_dioxide"),
            F.col("s.ozone"),

            F.col("s.ingestion_timestamp")
        )
    )

    # ========================================================
    # Fact Table Validation
    # ========================================================

    fact_batch = (
        fact_batch
        .filter(F.col("location_key").isNotNull())
        .filter(F.col("date_key").isNotNull())
        .filter(F.col("source_key").isNotNull())
    )

    # ========================================================
    # Merge Fact Records
    # ========================================================

    fact_table = DeltaTable.forName(
        spark,
        "aqi_platform.gold.fact_air_quality"
    )

    (
        fact_table.alias("target")
        .merge(
            fact_batch.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        f"✅ Gold micro-batch {batch_id} completed"
    )


# ============================================================
# 4. Gold Streaming Checkpoint
# ============================================================

gold_checkpoint = (
    "/Volumes/aqi_platform/gold/streaming_checkpoints/"
    "gold_transformation"
)


# ============================================================
# 5. Start Gold AvailableNow Pipeline
# ============================================================

gold_query = (
    silver_stream
    .writeStream
    .foreachBatch(
        process_gold_batch
    )
    .outputMode("append")
    .option(
        "checkpointLocation",
        gold_checkpoint
    )
    .trigger(
        availableNow=True
    )
    .start()
)

print("✅ Gold AvailableNow pipeline started")


# ============================================================
# 6. Wait for Gold Pipeline to Complete
# ============================================================

gold_query.awaitTermination()

print("✅ Gold AvailableNow pipeline completed")