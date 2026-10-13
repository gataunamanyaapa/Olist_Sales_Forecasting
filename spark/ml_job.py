"""
Spark MLlib: Prediksi Review Score menggunakan Random Forest
Input: payment_value, delivery_days, freight_value, price
Output: prediksi review_score (1-5)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

HDFS = "hdfs://namenode:9000"

spark = SparkSession.builder \
    .appName("OlistMLReviewScore") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=" * 50)
print("Spark MLlib — Prediksi Review Score")
print("=" * 50)

# Load data clean
print("\n[1/4] Load data...")
df = spark.read.parquet(f"{HDFS}/data/clean/fact_orders")

# Feature engineering
print("[2/4] Feature engineering...")
df_ml = df.select(
    "payment_value", "freight_value", "price",
    "payment_installments", "delivery_days", "review_score"
).dropna()

df_ml = df_ml \
    .filter(col("review_score").between(1, 5)) \
    .filter(col("payment_value") > 0) \
    .filter(col("delivery_days") > 0)

print(f"  ✓ Dataset ML: {df_ml.count()} baris")

# Split train/test
train, test = df_ml.randomSplit([0.8, 0.2], seed=42)
print(f"  ✓ Train: {train.count()} | Test: {test.count()}")

# Pipeline
feature_cols = ["payment_value","freight_value","price",
                "payment_installments","delivery_days"]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features")
rf        = RandomForestRegressor(
    featuresCol="features", labelCol="review_score",
    numTrees=100, maxDepth=5, seed=42
)

pipeline = Pipeline(stages=[assembler, scaler, rf])

# Train
print("\n[3/4] Training model Random Forest...")
model = pipeline.fit(train)

# Evaluasi
print("[4/4] Evaluasi model...")
predictions = model.transform(test)
evaluator_rmse = RegressionEvaluator(labelCol="review_score", metricName="rmse")
evaluator_r2   = RegressionEvaluator(labelCol="review_score", metricName="r2")

rmse = evaluator_rmse.evaluate(predictions)
r2   = evaluator_r2.evaluate(predictions)

print(f"\n  ✓ RMSE : {rmse:.4f}")
print(f"  ✓ R²   : {r2:.4f}")

# Contoh prediksi
print("\nContoh prediksi vs aktual:")
predictions.select("review_score","prediction","payment_value","delivery_days") \
    .show(10, truncate=False)

# Simpan model
model.save(f"{HDFS}/models/rf_review_score")
print(f"\n✅ Model disimpan ke HDFS: /models/rf_review_score")
spark.stop()
