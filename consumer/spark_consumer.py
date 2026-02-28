"""
Spark Structured Streaming Consumer
Reads weather records from Kafka, applies:
  - Data cleaning
  - Windowed aggregations (tumbling 5-min windows)
  - ML anomaly detection (Isolation Forest)
and writes results to shared JSON files for the dashboard.
"""

import json
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    avg, max as spark_max, min as spark_min,
    count, stddev, round as spark_round,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from anomaly_detector import AnomalyDetector

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = "weather-data"
OUTPUT_DIR      = Path("/app/data")
CHECKPOINT_DIR  = OUTPUT_DIR / "checkpoints"
AGG_DIR         = OUTPUT_DIR / "aggregations"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
AGG_DIR.mkdir(parents=True, exist_ok=True)

# ─── Kafka JSON Schema ────────────────────────────────────────────────────────
WEATHER_SCHEMA = StructType([
    StructField("city",                StringType(),  True),
    StructField("country",             StringType(),  True),
    StructField("timestamp",           StringType(),  True),
    StructField("temperature",         DoubleType(),  True),
    StructField("feels_like",          DoubleType(),  True),
    StructField("temp_min",            DoubleType(),  True),
    StructField("temp_max",            DoubleType(),  True),
    StructField("humidity",            IntegerType(), True),
    StructField("pressure",            IntegerType(), True),
    StructField("wind_speed",          DoubleType(),  True),
    StructField("wind_deg",            IntegerType(), True),
    StructField("wind_gust",           DoubleType(),  True),
    StructField("clouds",              IntegerType(), True),
    StructField("visibility",          IntegerType(), True),
    StructField("weather_id",          IntegerType(), True),
    StructField("weather_main",        StringType(),  True),
    StructField("weather_description", StringType(),  True),
])


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _append_json(file: Path, new_records: list, max_size: int = 2000) -> None:
    """Append records to a JSON file, keeping the last `max_size` entries."""
    try:
        existing = json.loads(file.read_text()) if file.exists() else []
    except (json.JSONDecodeError, OSError):
        existing = []
    existing.extend(new_records)
    file.write_text(json.dumps(existing[-max_size:], default=str))


# ─── Batch processor ─────────────────────────────────────────────────────────
def process_batch(df: DataFrame, epoch_id: int) -> None:
    """
    Called for every micro-batch by Spark Structured Streaming.
    Steps:
      1. Data cleaning (filter out physically impossible values)
      2. Persist raw cleaned records → weather_records.json
      3. ML anomaly detection → anomalies.json
    """
    if df.rdd.isEmpty():
        return

    n_raw = df.count()

    # ── 1. Data Cleaning ──────────────────────────────────────────────────────
    cleaned = df.filter(
        col("temperature").between(-60, 60)
        & col("humidity").between(0, 100)
        & col("pressure").between(870, 1085)
        & (col("wind_speed") >= 0)
        & (col("visibility") >= 0)
    )
    n_clean = cleaned.count()
    logger.info(f"[Batch {epoch_id}] raw={n_raw} | after cleaning={n_clean}")

    if n_clean == 0:
        return

    records_df = cleaned.toPandas()

    # ── 2. Persist raw records ────────────────────────────────────────────────
    _append_json(
        OUTPUT_DIR / "weather_records.json",
        records_df.to_dict(orient="records"),
    )

    # ── 3. ML anomaly detection ───────────────────────────────────────────────
    try:
        detector  = AnomalyDetector(model_dir=str(OUTPUT_DIR))
        anomalies = detector.detect(records_df)
        if not anomalies.empty:
            _append_json(
                OUTPUT_DIR / "anomalies.json",
                anomalies.to_dict(orient="records"),
                max_size=500,
            )
            logger.info(f"[Batch {epoch_id}] Anomalies detected: {len(anomalies)}")
    except Exception as exc:
        logger.error(f"[Batch {epoch_id}] ML error: {exc}", exc_info=True)


# ─── Spark Session ────────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("WeatherStructuredStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .config("spark.sql.streaming.checkpointLocation", str(CHECKPOINT_DIR))
        .getOrCreate()
    )


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created — connecting to Kafka …")

    # ── Read from Kafka ───────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ────────────────────────────────────────────────────────────
    weather = (
        raw_stream
        .select(from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
    )

    # ── Windowed aggregations (5-min tumbling window) ─────────────────────────
    windowed = (
        weather
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("city"),
        )
        .agg(
            spark_round(avg("temperature"),  2).alias("avg_temp"),
            spark_round(spark_max("temperature"), 2).alias("max_temp"),
            spark_round(spark_min("temperature"), 2).alias("min_temp"),
            spark_round(stddev("temperature"), 2).alias("stddev_temp"),
            spark_round(avg("humidity"),     1).alias("avg_humidity"),
            spark_round(avg("pressure"),     1).alias("avg_pressure"),
            spark_round(avg("wind_speed"),   2).alias("avg_wind_speed"),
            spark_round(avg("clouds"),       1).alias("avg_clouds"),
            count("*").alias("record_count"),
        )
        .select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("city"),
            col("avg_temp"),
            col("max_temp"),
            col("min_temp"),
            col("stddev_temp"),
            col("avg_humidity"),
            col("avg_pressure"),
            col("avg_wind_speed"),
            col("avg_clouds"),
            col("record_count"),
        )
    )

    # ── Write windowed aggregations to JSON files ─────────────────────────────
    agg_query = (
        windowed.writeStream
        .outputMode("append")
        .format("json")
        .option("path", str(AGG_DIR))
        .option("checkpointLocation", str(CHECKPOINT_DIR / "agg"))
        .trigger(processingTime="60 seconds")
        .start()
    )

    # ── Write raw records + ML via foreachBatch ───────────────────────────────
    raw_query = (
        weather.writeStream
        .outputMode("append")
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR / "raw"))
        .trigger(processingTime="60 seconds")
        .start()
    )

    logger.info("Both streaming queries started. Awaiting termination …")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
