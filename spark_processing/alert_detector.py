from pyspark.sql.functions import col, from_json, when, lit, current_timestamp, to_json, struct
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

    # Lecture du topic Kafka brut
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", "iot-sensor-data")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parsing JSON
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

    # Règles d’alerte
    df_alerts = (
        df_parsed
        .withColumn(
            "alert_type",

            # --- TEMPÉRATURE SALLE SERVEUR ---
            when(
                (col("sensor_type") == "temperature") &
                (col("unit") == "celsius") &
                ((col("value") < 15) | (col("value") > 30)),
                lit("temperature_critical")
            )
            .when(
                (col("sensor_type") == "temperature") &
                (col("unit") == "celsius") &
                (col("value") > 27),
                lit("temperature_warning")
            )

            # --- HUMIDITÉ ---
            .when(
                (col("sensor_type") == "humidity") &
                (col("unit") == "percent") &
                ((col("value") < 30) | (col("value") > 70)),
                lit("humidity_critical")
            )
            .when(
                (col("sensor_type") == "humidity") &
                (col("unit") == "percent") &
                ((col("value") < 35) | (col("value") > 60)),
                lit("humidity_warning")
            )

            # --- PRESSION ATMOSPHÉRIQUE ---
            .when(
                (col("sensor_type") == "pressure") &
                (col("unit") == "hPa") &
                ((col("value") < 980) | (col("value") > 1040)),
                lit("pressure_critical")
            )
            .when(
                (col("sensor_type") == "pressure") &
                (col("unit") == "hPa") &
                ((col("value") < 995) | (col("value") > 1030)),
                lit("pressure_warning")
            )

            # --- BATTERIE ---
            .when(col("battery_level") < 20, lit("battery_critical"))
            .when(col("battery_level") < 40, lit("battery_warning"))

            # --- SIGNAL ---
            .when(col("signal_strength") < -75, lit("weak_signal_critical"))
            .when(col("signal_strength") < -70, lit("weak_signal_warning"))

            .otherwise(None)
        )
        .where(col("alert_type").isNotNull())
        .select(
            # Contexte capteur / localisation (pour BDD + Kafka)
            col("sensor_id"),
            col("sensor_type"),
            col("building"),
            col("floor"),
            col("room"),
            col("timestamp"),
            col("battery_level"),
            col("signal_strength"),

            # Alerte
            col("alert_type"),

            when(col("alert_type").like("%critical%"), "critical")
            .when(col("alert_type").like("%warning%"), "warning")
            .otherwise("info")
            .alias("severity"),

            when(col("alert_type") == "temperature_critical", lit(30))
            .when(col("alert_type") == "temperature_warning", lit(27))
            .when(col("alert_type") == "humidity_critical", lit(70))
            .when(col("alert_type") == "humidity_warning", lit(60))
            .when(col("alert_type") == "pressure_critical", lit(1040))
            .when(col("alert_type") == "pressure_warning", lit(1030))
            .when(col("alert_type") == "battery_critical", lit(20))
            .when(col("alert_type") == "battery_warning", lit(40))
            .when(col("alert_type") == "weak_signal_critical", lit(-75))
            .when(col("alert_type") == "weak_signal_warning", lit(-70))
            .otherwise(None)
            .alias("threshold_value"),

            col("value").alias("actual_value"),

            when(col("alert_type") == "temperature_critical", lit("Température CRITIQUE (>30°C ou <15°C)"))
            .when(col("alert_type") == "temperature_warning", lit("Température élevée (>27°C)"))
            .when(col("alert_type") == "humidity_critical", lit("Humidité CRITIQUE (<30% ou >70%)"))
            .when(col("alert_type") == "humidity_warning", lit("Humidité anormale (<35% ou >60%)"))
            .when(col("alert_type") == "pressure_critical", lit("Pression atmosphérique anormale (<980 ou >1040 hPa)"))
            .when(col("alert_type") == "pressure_warning", lit("Pression hors plage (<995 ou >1030 hPa)"))
            .when(col("alert_type") == "battery_critical", lit("Batterie CRITIQUE (<20%)"))
            .when(col("alert_type") == "battery_warning", lit("Batterie faible (<40%)"))
            .when(col("alert_type").like("weak_signal_%"), lit("Signal WiFi IoT faible"))
            .otherwise("Anomalie détectée")
            .alias("message"),

            col("timestamp").cast("timestamp").alias("triggered_at"),
            lit(None).cast("timestamp").alias("resolved_at"),
            lit("active").alias("status"),
            current_timestamp().alias("created_at"),
        )
    )

    # Flux Kafka vers topic iot-alert avec location + metadata (dérivés des colonnes de df_alerts)
    df_alerts_kafka = (
        df_alerts
        .select(
            col("sensor_id").cast("string").alias("key"),
            to_json(
                struct(
                    "sensor_id",
                    "sensor_type",
                    "timestamp",

                    struct(
                        "building",
                        "floor",
                        "room"
                    ).alias("location"),

                    struct(
                        "battery_level",
                        "signal_strength"
                    ).alias("metadata"),

                    "alert_type",
                    "severity",
                    "threshold_value",
                    "actual_value",
                    "message",
                    "triggered_at",
                    "status",
                )
            ).alias("value")
        )
    )

    print("Detected alerts schema:")
    query_kafka = (
        df_alerts_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("topic", "iot-alert")
        .option("checkpointLocation", "/tmp/checkpoints/iot_alerts_kafka")
        .outputMode("append")
        .start()
    )

    # Flux vers PostgreSQL (écrit toutes les colonnes de df_alerts dans la table alerts)
    query = (
        df_alerts.writeStream
        .foreachBatch(write_alerts_to_postgres)
        .outputMode("append")
        .start()
    )

    print("Alert detector stream started")
    return query, query_kafka
