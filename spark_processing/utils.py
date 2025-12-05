from pyspark.sql import SparkSession


def build_spark_session(app_name):
    
    jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1"
    
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", jars)
        .config("spark.jars.repositories", "https://repo1.maven.org/maven2")
        .getOrCreate()
    )
