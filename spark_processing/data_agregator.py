from dotenv import load_dotenv
import os
import re
from pyspark.sql import SparkSession , functions as F
from pyspark.sql.functions import from_json, col, window, col, avg, count, min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

load_dotenv()
# Fonction pour nettoyer complètement les caractères invisibles
def clean_env_value(value, default=""):
    if value is None:
        return default
    cleaned = re.sub(r'[^\x20-\x7E]', '', value)  # Garder seulement ASCII imprimable
    return cleaned.strip()

POSTGRES_HOST = os.getenv("POSTGRES_DB_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "iot_sensors")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

spark = (
    SparkSession.builder
    .appName("Data_aggregator")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.postgresql:postgresql:42.7.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True),
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "iot-sensor-data")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("d"))
      .select("d.*")
)

agg_df = parsed.withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id"),
        col("sensor_type")
    ) \
    .agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        count("*").alias("record_count")
    ) \
   .select(
      col("sensor_id"),
      col("sensor_type"),
      col("window.start").alias("window_start"),
      col("window.end").alias("window_end"),
      col("avg_value"),
      col("min_value"),
      col("max_value"),
      col("record_count").alias("count")
   )

def ecrire_aggreta_db(df, epoch_id):
    clean_port = ''.join(c for c in POSTGRES_PORT if c.isdigit())
    df_with_batch = df.select(
        col("sensor_id"),
        col("sensor_type"),
        col("window_start"),
        col("window_end"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("count")
    )
    df_with_batch.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{clean_port}/{POSTGRES_DB}") \
        .option("dbtable", "aggregated_stats") \
        .option("user",POSTGRES_USER) \
        .option("password",POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()



#query.awaitTermination()



def start_aggreg(spark):
    print(f"Starting Data agregator (Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB})")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "iot-sensor-data")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        df.select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
    )

    agg_df = parsed.withWatermark("timestamp", "2 minutes") \
        .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id"),
        col("sensor_type")
    ) \
        .agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        count("*").alias("record_count")
    ) \
        .select(
        col("sensor_id"),
        col("sensor_type"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("record_count").alias("count")
    )


    query = (
        agg_df.writeStream
        .format("console")
        .trigger(processingTime="5 minutes")
        .outputMode("append")
        .option("truncate", "false")
        .foreachBatch(ecrire_aggreta_db)
        .start()
    )
    print("Data agregator stream started")
    return query