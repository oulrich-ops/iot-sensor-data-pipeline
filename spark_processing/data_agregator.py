import os
from dotenv import load_dotenv
from pyspark.sql.functions import col, from_json, window, avg, count, min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_IOT_SENSOR")

POSTGRES_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

print(f"PostgreSQL Config: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# Schema JSON des messages Kafka
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True),
])


def write_aggregates_to_postgres(batch_df, batch_id):
    try:
        if batch_df.count() == 0:
            print(f"[Batch {batch_id}] No aggregates to write")
            return
        
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("dbtable", "aggregated_stats") \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"[Batch {batch_id}] Successfully wrote {batch_df.count()} aggregates to PostgreSQL")
    except Exception as e:
        print(f"[Batch {batch_id}] Error writing aggregates to PostgreSQL: {e}")
        raise


def start_data_agregator(spark):
    """Start the Kafka -> aggregation -> Postgres stream using provided SparkSession.
    
    Returns the started StreamingQuery.
    """
    print(f"Starting data aggregator (Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB})")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    df_json = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select(
            col("data.sensor_id"),
            col("data.sensor_type"),
            col("data.timestamp"),
            col("data.value")
        ) \
        .withColumn("timestamp", col("timestamp").cast("timestamp"))

    agg_df = df_json.withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "4 minutes"),
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

    query = agg_df.writeStream \
        .foreachBatch(write_aggregates_to_postgres) \
        .outputMode("append") \
        .start()

    print("Data aggregator stream started")
    return query


if __name__ == "__main__":
    from spark_processing.utils import build_spark_session
    
    spark = build_spark_session("DataAggregator")
    q = start_data_agregator(spark)
    q.awaitTermination()
