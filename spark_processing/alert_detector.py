from pyspark.sql.functions import col, from_json, when, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")

POSTGRES_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# Schéma sensor
sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("location", StructType([
        StructField("building", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("room", IntegerType(), True),
    ]), True),
    StructField("timestamp", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("metadata", StructType([
        StructField("battery_level", IntegerType(), True),
        StructField("signal_strength", IntegerType(), True),
    ]), True),
])


def write_alerts_to_postgres(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            print(f"[Batch {batch_id}] No alerts to write")
            return

        (
            batch_df
            .write
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
            .option("dbtable", "alerts")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

        print(f"[Batch {batch_id}] Wrote {batch_df.count()} alerts to PostgreSQL")
    except Exception as e:
        print(f"[Batch {batch_id}] Error writing alerts to PostgreSQL: {e}")
        raise


def start_alert_detector(spark):
    """Create the alert detection stream using the provided SparkSession.

    Returns the started StreamingQuery.
    """
    print(f"Starting alert detector (Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB})")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", "iot-sensor-data")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = (
        df.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), sensor_schema).alias("data"))
          .select(
              col("data.sensor_id").alias("sensor_id"),
              col("data.sensor_type").alias("sensor_type"),
              col("data.location.building").alias("building"),
              col("data.location.floor").alias("floor"),
              col("data.location.room").alias("room"),
              col("data.timestamp").alias("timestamp"),
              col("data.value").alias("value"),
              col("data.unit").alias("unit"),
              col("data.metadata.battery_level").alias("battery_level"),
              col("data.metadata.signal_strength").alias("signal_strength"),
          )
    )

    df_alerts = (
        df_parsed
        .withColumn(
            "alert_type",
            when(
                (col("sensor_type") == "temperature") & (col("unit") == "celsius") &
                ((col("value") < 0) | (col("value") > 35)),
                lit("temperature_out_of_range")
            ).when(
                (col("sensor_type") == "humidity") & (col("unit") == "percent") &
                ((col("value") < 20) | (col("value") > 70)),
                lit("humidity_out_of_range")
            ).when(
                (col("sensor_type") == "pressure") & (col("unit") == "hPa") &
                ((col("value") < 950) | (col("value") > 1050)),
                lit("pressure_out_of_range")
            ).when(
                col("battery_level") < 30,
                lit("low_battery")
            ).when(
                col("signal_strength") < -75,
                lit("weak_signal")
            ).otherwise(lit(None))
        )
        .where(col("alert_type").isNotNull())
        .select(
            col("sensor_id"),
            col("alert_type"),
            when(col("alert_type").isin("temperature_out_of_range", "pressure_out_of_range"),
                 lit("critical")
            ).when(col("alert_type").isin("humidity_out_of_range", "low_battery"),
                   lit("warning")
            ).otherwise(lit("info")).alias("severity"),
            when(col("alert_type") == "temperature_out_of_range", lit(35.0))
            .when(col("alert_type") == "humidity_out_of_range", lit(70.0))
            .when(col("alert_type") == "pressure_out_of_range", lit(1050.0))
            .when(col("alert_type") == "low_battery", lit(30.0))
            .when(col("alert_type") == "weak_signal", lit(-75.0))
            .otherwise(lit(None).cast("double")).alias("threshold_value"),
            col("value").alias("actual_value"),
            when(col("alert_type") == "temperature_out_of_range",
                 lit("Température hors limites (0-35°C)"))
            .when(col("alert_type") == "humidity_out_of_range",
                 lit("Humidité hors limites (20-70%)"))
            .when(col("alert_type") == "pressure_out_of_range",
                 lit("Pression hors limites (950-1050 hPa)"))
            .when(col("alert_type") == "low_battery",
                 lit("Batterie faible (<30%)"))
            .when(col("alert_type") == "weak_signal",
                 lit("Signal trop faible (<-75 dBm)"))
            .otherwise(lit("Anomalie détectée")).alias("message"),
            col("timestamp").cast("timestamp").alias("triggered_at"),
            lit(None).cast("timestamp").alias("resolved_at"),
            lit("active").alias("status"),
            current_timestamp().alias("created_at")
        )
    )

    query = (
        df_alerts.writeStream
        .foreachBatch(write_alerts_to_postgres)
        .outputMode("append")
        .start()
    )

    print("Alert detector stream started")
    return query


if __name__ == "__main__":
    from spark_processing.utils import build_spark_session
    
    spark = build_spark_session("AlertDetector")
    q = start_alert_detector(spark)
    q.awaitTermination()
