"""
spark_ingest.py
===============
Reads raw nested JSON from GCS, flattens it, and writes to BigQuery staging tables.
 
Submitted to Dataproc by Kestra as:
  gcloud dataproc jobs submit pyspark gs://kbeauty-data-lake-end2end/jobs/spark_ingest.py \
    --cluster=kbeauty-cluster \
    --region=europe-west4 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    -- --date=2026-03-23
 
BigQuery tables written:
  kbeauty_pipeline.stg_orders
  kbeauty_pipeline.stg_order_items
  kbeauty_pipeline.stg_products
 
Environment variables (set in .env or Kestra):
  GCS_BUCKET   e.g. kbeauty-data-lake-end2end
  GCS_PROJECT  e.g. end2end-pipeline-kbeautystore
  BQ_DATASET   e.g. kbeauty_pipeline
"""
import os
import argparse
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
GCS_BUCKET  = os.getenv("GCS_BUCKET",  "kbeauty-data-lake-end2end")
BQ_PROJECT  = os.getenv("GCS_PROJECT", "end2end-pipeline-kbeautystore")
BQ_DATASET  = os.getenv("BQ_DATASET",  "kbeauty_pipeline")
BQ_TEMP_GCS = f"gs://{GCS_BUCKET}/bq_temp"


# ──────────────────────────────────────────
# SPARK SESSION
# ──────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("kbeauty-raw-ingest")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


# ──────────────────────────────────────────
# SCHEMAS  (enforce types on ingest)
# ──────────────────────────────────────────

ORDER_SCHEMA = StructType([
    StructField("order_id",           StringType(),    False),
    StructField("created_at",         StringType(),    True),   # parsed below
    StructField("channel",            StringType(),    True),
    StructField("order_status",       StringType(),    True),
    StructField("shipping_country",   StringType(),    True),
    StructField("customer", StructType([
        StructField("customer_id",    StringType(),    True),
        StructField("country_code",   StringType(),    True),
        StructField("country",        StringType(),    True),
        StructField("city",           StringType(),    True),
        StructField("age_group",      StringType(),    True),
        StructField("gender",         StringType(),    True),
        StructField("loyalty_tier",   StringType(),    True),
        StructField("signup_date",    StringType(),    True),
    ]), True),
    StructField("items", StringType(), True),   # array read as raw, exploded below
])


# ──────────────────────────────────────────
# INGEST ORDERS
# ──────────────────────────────────────────

def ingest_orders(spark: SparkSession, target_date: str):
    """
    Reads raw/orders/date=TARGET_DATE/orders.json from GCS.
    Produces two DataFrames:
      - stg_orders       (one row per order, customer fields flattened)
      - stg_order_items  (one row per line item, exploded from items array)
    """
    path = f"gs://{GCS_BUCKET}/raw/orders/date={target_date}/orders.json"
    print(f"Reading orders from: {path}")

    # Read as JSON array — each file is a JSON array of order objects
    raw = spark.read.option("multiline", "true").json(path)

    # ── stg_orders: flatten customer struct ──
    stg_orders = raw.select(
        F.col("order_id"),
        F.to_timestamp(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("order_timestamp"),
        F.to_date(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("order_date"),
        F.col("channel"),
        F.col("order_status"),
        F.col("shipping_country"),
        # flatten nested customer object
        F.col("customer.customer_id").alias("customer_id"),
        F.col("customer.country_code").alias("customer_country_code"),
        F.col("customer.country").alias("customer_country"),
        F.col("customer.city").alias("customer_city"),
        F.col("customer.age_group").alias("customer_age_group"),
        F.col("customer.gender").alias("customer_gender"),
        F.col("customer.loyalty_tier").alias("customer_loyalty_tier"),
        F.to_date(F.col("customer.signup_date")).alias("customer_signup_date"),
        # ingestion metadata
        F.lit(target_date).cast("date").alias("_ingestion_date"),
        F.current_timestamp().alias("_ingested_at"),
    ).dropDuplicates(["order_id"])

    # ── stg_order_items: explode items array ──
    stg_order_items = raw.select(
        F.col("order_id"),
        F.to_date(F.col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("order_date"),
        F.explode(F.col("items")).alias("item"),
    ).select(
        F.col("order_id"),
        F.col("order_date"),
        F.col("item.line_item_id").alias("line_item_id"),
        F.col("item.product_id").alias("product_id"),
        F.col("item.sku").alias("sku"),
        F.col("item.product_name").alias("product_name"),
        F.col("item.brand").alias("brand"),
        F.col("item.category").alias("category"),
        F.col("item.subcategory").alias("subcategory"),
        F.col("item.quantity").cast(IntegerType()).alias("quantity"),
        F.col("item.original_price").cast(DoubleType()).alias("original_price"),
        F.col("item.sale_price").cast(DoubleType()).alias("sale_price"),
        F.col("item.discount_pct").cast(DoubleType()).alias("discount_pct"),
        F.col("item.cost_price").cast(DoubleType()).alias("cost_price"),
        F.col("item.currency").alias("currency"),
        # derived
        (F.col("item.sale_price") * F.col("item.quantity")).alias("line_total"),
        (F.col("item.cost_price") * F.col("item.quantity")).alias("line_cost"),
        # ingestion metadata
        F.lit(target_date).cast("date").alias("_ingestion_date"),
        F.current_timestamp().alias("_ingested_at"),
    ).dropDuplicates(["line_item_id"])

    print(f"  stg_orders:      {stg_orders.count():,} rows")
    print(f"  stg_order_items: {stg_order_items.count():,} rows")
    return stg_orders, stg_order_items


# ──────────────────────────────────────────
# INGEST PRODUCTS
# ──────────────────────────────────────────

def ingest_products(spark: SparkSession, target_date: str):
    """
    Reads raw/products/snapshot_date=TARGET_DATE/products.json
    Produces stg_products (daily snapshot, used for SCD tracking in dbt)
    """
    path = f"gs://{GCS_BUCKET}/raw/products/snapshot_date={target_date}/products.json"
    print(f"Reading products from: {path}")

    raw = spark.read.option("multiline", "true").json(path)

    # The file has a top-level wrapper {"snapshot_date":..., "products":[...]}
    stg_products = raw.select(
        F.explode(F.col("products")).alias("p")
    ).select(
        F.col("p.product_id").alias("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.brand").alias("brand"),
        F.col("p.category").alias("category"),
        F.col("p.subcategory").alias("subcategory"),
        F.col("p.unit_price").cast(DoubleType()).alias("unit_price"),
        F.col("p.cost_price").cast(DoubleType()).alias("cost_price"),
        F.col("p.sku").alias("sku"),
        F.col("p.is_active").cast(BooleanType()).alias("is_active"),
        F.col("p.currency").alias("currency"),
        F.lit(target_date).cast("date").alias("snapshot_date"),
        F.current_timestamp().alias("_ingested_at"),
    )

    print(f"  stg_products:    {stg_products.count():,} rows")
    return stg_products


# ──────────────────────────────────────────
# WRITE TO BIGQUERY
# ──────────────────────────────────────────

def write_bq(df, table_name: str, mode: str = "append"):
    (
        df.write.format("bigquery")
        .option("table", f"{BQ_PROJECT}:{BQ_DATASET}.{table_name}")
        .option("temporaryGcsBucket", GCS_BUCKET)
        .option("partitionField", "order_date" if "order_date" in df.columns else "snapshot_date")
        .option("partitionType", "DAY")
        .option("writeDisposition", "WRITE_TRUNCATE")  # overwrite the partition
        .mode("overwrite")
        .save()
    )

# ──────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=str(date.today() - timedelta(days=1)),
        help="Processing date YYYY-MM-DD (default: yesterday)"
    )
    args = parser.parse_args()
    target_date = args.date

    print(f"\n⚡  Spark Ingest — {target_date}\n")

    spark = get_spark()

    # 1. Ingest + flatten
    stg_orders, stg_order_items = ingest_orders(spark, target_date)
    stg_products                = ingest_products(spark, target_date)

    # 2. Write to BigQuery (append daily partitions)
    write_bq(stg_orders,      "stg_orders",      mode="append")
    write_bq(stg_order_items, "stg_order_items", mode="append")
    write_bq(stg_products,    "stg_products",    mode="append")

    print(f"\n✅  Ingest complete for {target_date}\n")
    spark.stop()


if __name__ == "__main__":
    main()