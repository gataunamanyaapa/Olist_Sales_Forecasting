#!/bin/bash
# ============================================
# Script: Upload semua CSV Olist ke HDFS
# ============================================

echo "=== Membuat direktori HDFS ==="
docker exec namenode hdfs dfs -mkdir -p /data/raw
docker exec namenode hdfs dfs -mkdir -p /data/staging
docker exec namenode hdfs dfs -mkdir -p /data/clean
docker exec namenode hdfs dfs -chmod -R 777 /data
echo "Direktori HDFS berhasil dibuat."
echo ""

echo "=== Upload CSV ke HDFS ==="
FILES=(
  "olist_customers_dataset.csv"
  "olist_geolocation_dataset.csv"
  "olist_order_items_dataset.csv"
  "olist_order_payments_dataset.csv"
  "olist_order_reviews_dataset.csv"
  "olist_orders_dataset.csv"
  "olist_products_dataset.csv"
  "olist_sellers_dataset.csv"
  "product_category_name_translation.csv"
)

for fname in "${FILES[@]}"; do
  if [ -f "data/raw/$fname" ]; then
    docker cp "data/raw/$fname" namenode:/tmp/$fname
    docker exec namenode hdfs dfs -put -f /tmp/$fname /data/raw/
    echo "  ✓ $fname"
  else
    echo "  ✗ TIDAK DITEMUKAN: $fname"
  fi
done

echo ""
echo "=== Verifikasi upload ==="
docker exec namenode hdfs dfs -ls /data/raw/
echo ""
echo "=== Selesai! ==="
