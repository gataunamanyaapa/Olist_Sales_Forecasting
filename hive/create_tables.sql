-- ============================================
-- Hive DDL: Data Warehouse Olist
-- ============================================

CREATE DATABASE IF NOT EXISTS olist;
USE olist;

-- Dimensi Customers
CREATE EXTERNAL TABLE IF NOT EXISTS dim_customers (
    customer_id         STRING,
    customer_unique_id  STRING,
    customer_zip_code   STRING,
    customer_city       STRING,
    customer_state      STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/clean/dim_customers';

-- Dimensi Products
CREATE EXTERNAL TABLE IF NOT EXISTS dim_products (
    product_id                      STRING,
    product_category_name           STRING,
    product_category_name_english   STRING,
    product_weight_g                FLOAT,
    product_length_cm               FLOAT,
    product_height_cm               FLOAT,
    product_width_cm                FLOAT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/clean/dim_products';

-- Dimensi Sellers
CREATE EXTERNAL TABLE IF NOT EXISTS dim_sellers (
    seller_id               STRING,
    seller_zip_code_prefix  STRING,
    seller_city             STRING,
    seller_state            STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/clean/dim_sellers';

-- Tabel Fakta Orders
CREATE EXTERNAL TABLE IF NOT EXISTS fact_orders (
    order_id                        STRING,
    customer_id                     STRING,
    order_status                    STRING,
    order_purchase_timestamp        TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    product_id                      STRING,
    seller_id                       STRING,
    price                           FLOAT,
    freight_value                   FLOAT,
    payment_type                    STRING,
    payment_installments            INT,
    payment_value                   FLOAT,
    review_score                    INT,
    customer_city                   STRING,
    customer_state                  STRING,
    delivery_days                   INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/clean/fact_orders';

-- ─── QUERY ANALITIK ─────────────────────────────────

-- 1. Top 10 kategori produk terlaris
SELECT p.product_category_name_english,
       COUNT(*) as total_orders,
       ROUND(SUM(f.price), 2) as total_revenue
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_category_name_english
ORDER BY total_orders DESC
LIMIT 10;

-- 2. Revenue per bulan
SELECT DATE_FORMAT(order_purchase_timestamp, 'yyyy-MM') as bulan,
       COUNT(DISTINCT order_id) as total_orders,
       ROUND(SUM(payment_value), 2) as total_revenue
FROM fact_orders
WHERE order_purchase_timestamp IS NOT NULL
GROUP BY DATE_FORMAT(order_purchase_timestamp, 'yyyy-MM')
ORDER BY bulan;

-- 3. Rata-rata waktu pengiriman per state
SELECT customer_state,
       COUNT(*) as total_orders,
       ROUND(AVG(delivery_days), 1) as avg_delivery_days
FROM fact_orders
WHERE delivery_days IS NOT NULL AND delivery_days > 0
GROUP BY customer_state
ORDER BY avg_delivery_days ASC;

-- 4. Distribusi review score
SELECT review_score,
       COUNT(*) as total,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
FROM fact_orders
WHERE review_score IS NOT NULL
GROUP BY review_score
ORDER BY review_score;

-- 5. Metode pembayaran terpopuler
SELECT payment_type,
       COUNT(*) as total,
       ROUND(AVG(payment_installments), 1) as avg_installments,
       ROUND(SUM(payment_value), 2) as total_revenue
FROM fact_orders
WHERE payment_type IS NOT NULL
GROUP BY payment_type
ORDER BY total DESC;

-- 6. Top 10 kota dengan order terbanyak
SELECT customer_city, customer_state,
       COUNT(*) as total_orders,
       ROUND(AVG(review_score), 2) as avg_review
FROM fact_orders
WHERE customer_city IS NOT NULL
GROUP BY customer_city, customer_state
ORDER BY total_orders DESC
LIMIT 10;
