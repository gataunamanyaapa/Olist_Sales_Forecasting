"""
Spark Structured Streaming: Konsumsi data dari Kafka topic 'olist-orders'
Proses realtime dan simpan ke HDFS
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, sum as _sum
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, TimestampType
)

HDFS  = "hdfs://namenode:9000"
KAFKA = "kafka:29092"

spark = SparkSession.builder \
    .appName("OlistStreamProcessing") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema data dari Kafka
schema = StructType([
    StructField("order_id",        StringType()),
    StructField("customer_id",     StringType()),
    StructField("order_status",    StringType()),
    StructField("customer_city",   StringType()),
    StructField("customer_state",  StringType()),
    StructField("product_category",StringType()),
    StructField("payment_type",    StringType()),
    StructField("payment_value",   FloatType()),
    StructField("freight_value",   FloatType()),
    StructField("review_score",    IntegerType()),
    StructField("timestamp",       StringType()),
])

# Baca dari Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA) \
    .option("subscribe", "olist-orders") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
df_orders = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
 .withColumn("event_time", col("timestamp").cast(TimestampType()))

# Agregasi per window 1 menit
df_agg = df_orders \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("customer_state")) \
    .agg(
        count("order_id").alias("total_orders"),
        _sum("payment_value").alias("total_revenue"),
        avg("review_score").alias("avg_review")
    )

# Output ke console (untuk demo)
query_console = df_orders.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .start()

# Output ke HDFS
query_hdfs = df_orders.writeStream \
    .format("parquet") \
    .option("path", f"{HDFS}/data/streaming/orders") \
    .option("checkpointLocation", f"{HDFS}/checkpoints/orders") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ Spark Streaming berjalan... (Ctrl+C untuk stop)")
query_console.awaitTermination()
