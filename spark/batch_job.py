"""
Spark Batch ETL: Raw -> Staging -> Clean
Dataset: Olist E-Commerce Brazil
Fixed: use try_cast for all numeric/int columns (Spark 4.x safe)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_timestamp, datediff, expr
)

HDFS = "hdfs://namenode:9000"

spark = SparkSession.builder \
    .appName("OlistBatchETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 50)
print("Spark Batch ETL — Olist Dataset")
print("=" * 50)

# ─── RAW LAYER ───────────────────────────────────────
print("\n[1/3] Membaca data dari HDFS Raw Layer...")

orders    = spark.read.csv(f"{HDFS}/data/raw/olist_orders_dataset.csv",          header=True, inferSchema=False)
items     = spark.read.csv(f"{HDFS}/data/raw/olist_order_items_dataset.csv",      header=True, inferSchema=False)
customers = spark.read.csv(f"{HDFS}/data/raw/olist_customers_dataset.csv",        header=True, inferSchema=False)
products  = spark.read.csv(f"{HDFS}/data/raw/olist_products_dataset.csv",         header=True, inferSchema=False)
reviews   = spark.read.csv(f"{HDFS}/data/raw/olist_order_reviews_dataset.csv",    header=True, inferSchema=False)
payments  = spark.read.csv(f"{HDFS}/data/raw/olist_order_payments_dataset.csv",   header=True, inferSchema=False)
sellers   = spark.read.csv(f"{HDFS}/data/raw/olist_sellers_dataset.csv",          header=True, inferSchema=False)
category  = spark.read.csv(f"{HDFS}/data/raw/product_category_name_translation.csv", header=True, inferSchema=False)

print(f"  ✓ orders   : {orders.count()} baris")
print(f"  ✓ items    : {items.count()} baris")
print(f"  ✓ customers: {customers.count()} baris")
print(f"  ✓ products : {products.count()} baris")
print(f"  ✓ reviews  : {reviews.count()} baris")
print(f"  ✓ payments : {payments.count()} baris")

# ─── STAGING LAYER ───────────────────────────────────
print("\n[2/3] Staging Layer — Membersihkan data...")

# Orders: parse tanggal dengan to_timestamp
orders_staging = orders \
    .dropna(subset=["order_id","customer_id"]) \
    .withColumn("order_purchase_timestamp",
        to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("order_delivered_customer_date",
        to_timestamp(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss")) \
    .filter(col("order_status").isin(
        ["delivered","shipped","approved","processing","created","canceled"]))

# Items: try_cast untuk float agar aman
items_staging = items \
    .dropna(subset=["order_id","product_id","price"]) \
    .withColumn("price",         expr("try_cast(price as float)")) \
    .withColumn("freight_value", expr("try_cast(freight_value as float)")) \
    .filter(col("price") > 0)

# Products: join kategori
products_staging = products \
    .join(category, "product_category_name", "left") \
    .withColumn("product_category_name_english",
        when(col("product_category_name_english").isNull(), "others")
        .otherwise(col("product_category_name_english")))

# Reviews: try_cast review_score — tolerate nilai aneh
reviews_staging = reviews \
    .dropna(subset=["order_id","review_score"]) \
    .withColumn("review_score", expr("try_cast(review_score as integer)")) \
    .filter(col("review_score").isNotNull()) \
    .filter(col("review_score").between(1, 5))

# Payments: try_cast semua numerik
payments_staging = payments \
    .dropna(subset=["order_id","payment_value"]) \
    .withColumn("payment_value",        expr("try_cast(payment_value as float)")) \
    .withColumn("payment_installments", expr("try_cast(payment_installments as integer)")) \
    .filter(col("payment_value").isNotNull()) \
    .filter(col("payment_value") > 0)

print(f"  ✓ orders_staging  : {orders_staging.count()} baris")
print(f"  ✓ items_staging   : {items_staging.count()} baris")
print(f"  ✓ reviews_staging : {reviews_staging.count()} baris")
print(f"  ✓ payments_staging: {payments_staging.count()} baris")

# Simpan staging ke HDFS
orders_staging.write.mode("overwrite").parquet(f"{HDFS}/data/staging/orders")
items_staging.write.mode("overwrite").parquet(f"{HDFS}/data/staging/items")
customers.write.mode("overwrite").parquet(f"{HDFS}/data/staging/customers")
products_staging.write.mode("overwrite").parquet(f"{HDFS}/data/staging/products")
reviews_staging.write.mode("overwrite").parquet(f"{HDFS}/data/staging/reviews")
payments_staging.write.mode("overwrite").parquet(f"{HDFS}/data/staging/payments")
sellers.write.mode("overwrite").parquet(f"{HDFS}/data/staging/sellers")
print("  ✓ Semua staging tersimpan ke HDFS")

# ─── CLEAN LAYER ─────────────────────────────────────
print("\n[3/3] Clean Layer — Membangun Data Warehouse...")

# Dimensi
customers.write.mode("overwrite").parquet(f"{HDFS}/data/clean/dim_customers")
products_staging.write.mode("overwrite").parquet(f"{HDFS}/data/clean/dim_products")
sellers.write.mode("overwrite").parquet(f"{HDFS}/data/clean/dim_sellers")

# Tabel Fakta: join semua tabel
fact_orders = orders_staging \
    .join(items_staging.select(
        "order_id","product_id","seller_id","price","freight_value"),
        "order_id", "left") \
    .join(payments_staging.select(
        "order_id","payment_type","payment_installments","payment_value"),
        "order_id", "left") \
    .join(reviews_staging.select(
        "order_id","review_score"), "order_id", "left") \
    .join(customers.select(
        "customer_id","customer_city","customer_state"), "customer_id", "left") \
    .withColumn("delivery_days",
        when(
            col("order_delivered_customer_date").isNotNull() &
            col("order_purchase_timestamp").isNotNull(),
            datediff(col("order_delivered_customer_date"),
                     col("order_purchase_timestamp"))
        ).otherwise(None))

fact_orders.write.mode("overwrite").parquet(f"{HDFS}/data/clean/fact_orders")

total = fact_orders.count()
print(f"  ✓ dim_customers, dim_products, dim_sellers selesai")
print(f"  ✓ fact_orders: {total} baris")
print("\n✅ Batch ETL selesai! Data siap untuk analisis.")
spark.stop()
