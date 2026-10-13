from pyspark.sql import SparkSession

HDFS = 'hdfs://namenode:9000'
spark = SparkSession.builder.appName('ExportCSV').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df = spark.read.parquet(f'{HDFS}/data/clean/fact_orders')
print('Kolom:', df.columns)
print('Total rows:', df.count())

# Pilih kolom yang penting saja
df_export = df.select(
    'order_id','order_status','order_purchase_timestamp',
    'price','freight_value','payment_type',
    'payment_value','review_score','customer_state','delivery_days'
).na.fill({'review_score': 0, 'delivery_days': 0, 'price': 0})

df_export.coalesce(1).write.mode('overwrite') \
    .option('header', 'true') \
    .csv(f'{HDFS}/data/export/fact_orders_export')

print('Export selesai!')
spark.stop()
