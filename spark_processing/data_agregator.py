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

POSTGRES_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
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
"""
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

"""
def ecrire_aggreta_db(df, epoch_id):
    # Nettoyer le port au cas où
    clean_port = ''.join(c for c in POSTGRES_PORT if c.isdigit())

    # 1) Récupérer les lignes du micro-batch côté driver
    rows = df.select(
        "sensor_id",
        "sensor_type",
        "window_start",
        "window_end",
        "avg_value",
        "min_value",
        "max_value",
        "count"
    ).collect()

    if not rows:
        return

    # 2) Construire la clause VALUES (...) , (...) , ...
    values_sql_parts = []
    for r in rows:
        sensor_id = r["sensor_id"].replace("'", "''")
        sensor_type = r["sensor_type"].replace("'", "''")

        window_start = r["window_start"].strftime("%Y-%m-%d %H:%M:%S")
        window_end = r["window_end"].strftime("%Y-%m-%d %H:%M:%S")

        avg_value = "NULL" if r["avg_value"] is None else str(float(r["avg_value"]))
        min_value = "NULL" if r["min_value"] is None else str(float(r["min_value"]))
        max_value = "NULL" if r["max_value"] is None else str(float(r["max_value"]))
        count_val = "NULL" if r["count"] is None else str(int(r["count"]))

        values_sql_parts.append(
            f"('{sensor_id}', '{sensor_type}', "
            f"'{window_start}', '{window_end}', "
            f"{avg_value}, {min_value}, {max_value}, {count_val})"
        )

    values_sql = ",\n".join(values_sql_parts)

    # 3) Requête UPSERT vers aggregated_stats
    upsert_sql = f"""
        INSERT INTO aggregated_stats (
            sensor_id,
            sensor_type,
            window_start,
            window_end,
            avg_value,
            min_value,
            max_value,
            count
        )
        VALUES
        {values_sql}
        ON CONFLICT (sensor_id, window_start)
        DO UPDATE SET
            sensor_type = EXCLUDED.sensor_type,
            window_end  = EXCLUDED.window_end,
            avg_value   = EXCLUDED.avg_value,
            min_value   = EXCLUDED.min_value,
            max_value   = EXCLUDED.max_value,
            count       = EXCLUDED.count;
    """

    jvm = spark._jvm
    DriverManager = jvm.java.sql.DriverManager

    url = f"jdbc:postgresql://{POSTGRES_HOST}:{clean_port}/{POSTGRES_DB}"
    conn = None
    stmt = None
    try:
        conn = DriverManager.getConnection(url, POSTGRES_USER, POSTGRES_PASSWORD)
        stmt = conn.createStatement()
        stmt.execute(upsert_sql)
        # pas de conn.commit() ni conn.rollback()
    except Exception as e:
        print(f"Erreur lors de l'UPSERT direct dans aggregated_stats : {e}")
        raise
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


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