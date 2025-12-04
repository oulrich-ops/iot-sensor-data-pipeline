import os
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

load_dotenv()


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_IOT_SENSOR")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")

POSTGRES_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

print(f"PostgreSQL Config: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# Schema JSON des messages Kafka
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("sensor_type", StringType()),
    StructField("value", FloatType()),
    StructField("unit", StringType()),
    StructField("timestamp", StringType()),
    StructField("location", StructType([
        StructField("building", StringType()),
        StructField("floor", IntegerType()),
        StructField("room", StringType())
    ])),
    StructField("metadata", StructType([
        StructField("battery_level", IntegerType()),
        StructField("signal_strength", IntegerType())
    ]))
])

spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
    .config("spark.jars.repositories", "https://repo1.maven.org/maven2") \
    .getOrCreate()

# Lecture continue depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Les données Kafka sont en bytes dans 'value'
df_json = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.sensor_id"),
        col("data.sensor_type"),
        col("data.value"),
        col("data.unit"),
        col("data.timestamp"),
        col("data.location.building"),
        col("data.location.floor"),
        col("data.location.room"),
        col("data.metadata.battery_level"),
        col("data.metadata.signal_strength")
    ) \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# Fonction pour écrire dans PostgreSQL
def write_to_postgres(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            print(f"[Batch {batch_id}] No data to write")
            return
        
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("dbtable", "sensor_readings") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"[Batch {batch_id}] Successfully wrote {batch_df.count()} rows to PostgreSQL")
    except Exception as e:
        print(f"[Batch {batch_id}] Error writing to PostgreSQL: {e}")
        raise

# Stream continu
query = df_json.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

query.awaitTermination()
