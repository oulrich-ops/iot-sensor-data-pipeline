from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, to_json, struct, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

spark = (
    SparkSession.builder
    .appName("AlertDetector")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1"
    )
    .config("spark.jars.repositories", "https://repo1.maven.org/maven2")
    .getOrCreate()
)

sensor_schema = StructType([
    StructField("sensor_id",   StringType(),  True),
    StructField("sensor_type", StringType(),  True),
    StructField("location", StructType([
        StructField("building", StringType(),  True),
        StructField("floor",    IntegerType(), True),
        StructField("room",     IntegerType(), True),
    ]), True),
    StructField("timestamp",   StringType(),  True),
    StructField("value",       DoubleType(),  True),
    StructField("unit",        StringType(),  True),
    StructField("metadata", StructType([
        StructField("battery_level",   IntegerType(), True),
        StructField("signal_strength", IntegerType(), True),
    ]), True),
])

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

df_alerts_formatted = (
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
    # on ne garde que les lignes avec alerte
    .where(col("alert_type").isNotNull())
    # on fabrique directement les colonnes de la table alerts
    .select(
        col("sensor_id"),
        col("alert_type"),
        when(col("alert_type").isin("temperature_out_of_range",
                                    "pressure_out_of_range"),
             lit("critical")
        ).when(col("alert_type").isin("humidity_out_of_range",
                                      "low_battery"),
               lit("warning")
        ).otherwise(lit("info")).alias("severity"),
        when(col("alert_type") == "temperature_out_of_range",
             lit(35.0)
        ).when(col("alert_type") == "humidity_out_of_range",
               lit(70.0)
        ).when(col("alert_type") == "pressure_out_of_range",
               lit(1050.0)
        ).otherwise(lit(None).cast("double")).alias("threshold_value"),
        col("value").alias("actual_value"),
        when(col("alert_type") == "temperature_out_of_range",
             lit("Temperature out of allowed range")
        ).when(col("alert_type") == "humidity_out_of_range",
               lit("Humidity out of allowed range")
        ).when(col("alert_type") == "pressure_out_of_range",
               lit("Pressure out of allowed range")
        ).when(col("alert_type") == "low_battery",
               lit("Battery level too low")
        ).when(col("alert_type") == "weak_signal",
               lit("Signal strength too weak")
        ).otherwise(lit("Anomaly detected")).alias("message"),
        col("timestamp").alias("triggered_at"),
        lit(None).cast("timestamp").alias("resolved_at"),
        lit("active").alias("status"),
        current_timestamp().alias("created_at")
    )
)

# Production vers Kafka au format alerts
df_kafka = (
    df_alerts_formatted
    .select(
        col("sensor_id").cast("string").alias("key"),
        to_json(
            struct(
                "sensor_id",
                "alert_type",
                "severity",
                "threshold_value",
                "actual_value",
                "message",
                "triggered_at",
                "resolved_at",
                "status",
                "created_at"
            )
        ).alias("value")
    )
)

query = (
    df_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("topic", "iot-alert")
    .option("checkpointLocation", "file:///C:/tmp/spark_checkpoints/iot_alerts")
    .outputMode("append")
    .start()
)

query.awaitTermination()
