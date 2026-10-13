# Olist Big Data Pipeline

A Docker-based big data project for exploring Brazilian Olist e-commerce data. It includes HDFS storage, Spark batch ETL and machine learning, a Kafka streaming demo, and example warehouse queries for analysis.

## Technology stack

- Hadoop HDFS for distributed storage
- Apache Spark for batch processing, streaming, and MLlib
- Apache Kafka and ZooKeeper for the streaming demo
- Apache Superset for dashboards and data exploration
- Docker Compose for running the services

## Project structure

```text
data/
  raw/                 Olist source CSV files
  fact_orders.csv      Example fact table export
  fact_orders_clean.csv
hive/
  create_tables.sql    Example warehouse DDL and analytical queries
kafka/
  producer.py          Sends simulated order events to Kafka
scripts/
  ingest.sh            Uploads raw CSV files to HDFS (Bash)
  duplicate_data.py    Creates larger copies of the CSV files
spark/
  batch_job.py         Batch ETL from raw CSVs to Parquet
  stream_job.py        Kafka Structured Streaming example
  ml_job.py            Review-score prediction with Spark MLlib
  export_csv.py        Exports the cleaned fact table
docker-compose.yml     Big data service definitions
requirements.txt       Python package requirements
```

## Requirements

- Docker Desktop with Docker Compose
- The Olist CSV files in `data/raw/`
- Python and the packages in `requirements.txt` only for scripts run on the host

## Run the batch ETL (Windows PowerShell)

Run these commands from the project directory.

1. Start the services:

   ```powershell
   docker compose up -d
   docker compose ps
   ```

2. Create HDFS folders and upload the raw CSV files:

   ```powershell
   docker exec namenode hdfs dfs -mkdir -p /data/raw /data/staging /data/clean

   Get-ChildItem .\data\raw\*.csv | ForEach-Object {
       docker cp $_.FullName "namenode:/tmp/$($_.Name)"
       docker exec namenode hdfs dfs -put -f "/tmp/$($_.Name)" /data/raw/
   }
   ```

3. Submit the Spark job:

   ```powershell
   docker cp .\spark\batch_job.py spark-master:/tmp/batch_job.py
   docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /tmp/batch_job.py
   ```

The batch job reads CSV files from `/data/raw` in HDFS and writes staging and cleaned Parquet data under `/data/staging` and `/data/clean`.

## Service interfaces

- HDFS NameNode: <http://localhost:9870>
- Spark Master: <http://localhost:8080>
- Superset: <http://localhost:8088>

Superset starts as a service, but this Compose configuration does not initialize its metadata database or configure a connection to the ETL output. Complete the Superset setup and configure a queryable data source before creating dashboards.

## Optional components

The Kafka producer can be run from the project root after installing its dependency:

```powershell
python -m pip install kafka-python==2.0.2
python .\kafka\producer.py
```

`scripts/ingest.sh` requires Bash, such as Git Bash or WSL. `scripts/duplicate_data.py` multiplies the raw CSV data and requires pandas; run it only if you intentionally want larger input files.

## Current setup notes

- The Compose file does not define a Hive service. `hive/create_tables.sql` is an example of table definitions and queries; it cannot be executed against Hive until a Hive service or compatible SQL engine is configured.
- The streaming job currently specifies Kafka at `kafka:29092`, while the Compose Kafka service exposes `9092` and advertises `localhost`. Align the Kafka listener configuration and the Spark bootstrap-server setting before running the streaming example.
- The Compose file uses `apache/spark:latest`, while the Python requirements pin PySpark 3.3.0 and the streaming connector is also set to Spark 3.3.0. Keep the Spark runtime and connector versions aligned if running these jobs.

## Stop the services

```powershell
docker compose down
```

This stops and removes the containers but preserves the named HDFS volumes. To also delete the stored HDFS data, use `docker compose down -v` only when you intend to remove it.
