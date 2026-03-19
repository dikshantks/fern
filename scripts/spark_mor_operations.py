#!/usr/bin/env python3
"""
Spark script for Iceberg MOR (Merge-on-Read) operations.

Creates delete.parquet files (equality + positional) and MERGE INTO.
Use with Hive metastore + MinIO.

Usage:
    ./scripts/run_spark_mor.sh

Or manually:
    spark-submit --driver-memory 8g \\
      --conf spark.driver.maxResultSize=8g \\
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,software.amazon.awssdk:bundle:2.25.61,software.amazon.awssdk:url-connection-client:2.25.61 \\
      --conf spark.driver.host=127.0.0.1 \\
      --conf spark.driver.bindAddress=127.0.0.1 \\
      scripts/spark_mor_operations.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DoubleType, StringType, TimestampType
from datetime import datetime, timedelta
import random


def get_spark():
    """Build SparkSession with Iceberg + Hive + MinIO."""
    return (
        SparkSession.builder
        .appName("Iceberg MOR Operations")
        # Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://localhost:9083")
        # MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Driver for local run
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def create_mor_orders_table(spark):
    """Create or replace MOR orders table."""
    print("\n--- Creating MOR orders table ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql("DROP TABLE IF EXISTS demo.orders")

    spark.sql("""
        CREATE TABLE demo.orders (
            id BIGINT NOT NULL,
            order_date TIMESTAMP NOT NULL,
            customer_id BIGINT NOT NULL,
            product_id BIGINT NOT NULL,
            quantity INT NOT NULL,
            amount DOUBLE NOT NULL,
            status STRING NOT NULL
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read'
        )
        LOCATION 's3a://warehouse/demo/orders'
    """)
    print("Created demo.orders (MOR)")


def generate_orders_df(spark, n, start_id=1, base_date=None):
    """Generate sample orders as DataFrame."""
    if base_date is None:
        base_date = datetime(2024, 6, 1)
    statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
    rows = []
    for i in range(n):
        rows.append((
            start_id + i,
            base_date + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23)),
            random.randint(1, 5000),
            random.randint(100, 999),
            random.randint(1, 10),
            round(random.uniform(10, 500), 2),
            random.choice(statuses),
        ))
    schema = StructType([
        StructField("id", LongType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("customer_id", LongType(), False),
        StructField("product_id", LongType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False),
    ])
    return spark.createDataFrame(rows, schema)


def run_mor_operations(spark):
    """Run MOR DML: insert, equality delete, positional delete, merge into."""
    print("\n--- MOR Operations ---")

    # 1. Initial insert
    print("\n[1/6] Initial insert (100 records)...")
    df1 = generate_orders_df(spark, 100, start_id=1)
    df1.writeTo("demo.orders").append()
    print("    Done")

    # 2. Equality delete (by primary key -> equality delete files)
    print("\n[2/6] Equality delete: DELETE WHERE id IN (5, 10, 15, 20, 25)...")
    spark.sql("DELETE FROM demo.orders WHERE id IN (5, 10, 15, 20, 25)")
    print("    Done (equality delete files in s3a://warehouse/demo/orders/data/)")

    # 3. Positional delete (by non-key predicate -> positional delete files)
    print("\n[3/6] Positional delete: DELETE WHERE amount < 50...")
    spark.sql("DELETE FROM demo.orders WHERE amount < 50")
    print("    Done (positional delete files)")

    # 4. Append more orders
    print("\n[4/6] Append (50 records)...")
    df4 = generate_orders_df(spark, 50, start_id=101, base_date=datetime(2024, 6, 8))
    df4.writeTo("demo.orders").append()
    print("    Done")

    # 5. MERGE INTO (upsert)
    print("\n[5/6] MERGE INTO: upsert rows 30, 40, 50...")
    merge_df = spark.createDataFrame([
        (30, datetime(2024, 6, 10), 1001, 201, 2, 199.99, "SHIPPED"),
        (40, datetime(2024, 6, 11), 1002, 202, 3, 299.99, "DELIVERED"),
        (50, datetime(2024, 6, 12), 1003, 203, 1, 49.99, "CONFIRMED"),
    ], "id: long, order_date: timestamp, customer_id: long, product_id: long, quantity: int, amount: double, status: string")
    merge_df.createOrReplaceTempView("merge_source")

    spark.sql("""
        MERGE INTO demo.orders t
        USING merge_source s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("    Done")

    # 6. Positional delete by status
    print("\n[6/6] Positional delete: DELETE WHERE status = 'CANCELLED'...")
    spark.sql("DELETE FROM demo.orders WHERE status = 'CANCELLED'")
    print("    Done")

    # Summary
    count = spark.sql("SELECT COUNT(*) FROM demo.orders").collect()[0][0]
    print(f"\nFinal row count: {count}")
    print("Check MinIO: http://localhost:9001/browser/warehouse/demo%2Forders%2Fdata/")
    print("Look for *equality-deletes-*.parquet and *position-deletes-*.parquet")


def main():
    spark = get_spark()
    try:
        create_mor_orders_table(spark)
        run_mor_operations(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
