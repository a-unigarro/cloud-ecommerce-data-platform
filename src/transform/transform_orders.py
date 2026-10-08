"""Raw orders CSV -> processed Parquet.

Processed layout (mirrors raw / future S3):
    processed/olist/orders/ingestion_date=YYYY-MM-DD/part-*.parquet
"""
import argparse
import json
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.transform.raw_reader import latest_completed_date
from src.transform.schemas import ORDERS_SCHEMA
from src.transform.session import get_spark

log = logging.getLogger("transform_orders")
DATASET = "orders"


def read_raw_orders(spark: SparkSession, ingestion_date: str) -> DataFrame:
    path = config.RAW_DIR / DATASET / f"ingestion_date={ingestion_date}"
    return (spark.read
            .option("header", True)
            .option("mode", "FAILFAST") ### error on any malformed row, rather than silently dropping it
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(ORDERS_SCHEMA)
            .csv(str(path)))


def clean_orders(df: DataFrame, ingestion_date: str) -> DataFrame:
    """Standardize values. Never drops rows: null dates are real information."""
    return (df
            .withColumn("order_id", F.trim("order_id"))
            .withColumn("customer_id", F.trim("customer_id"))
            .withColumn("order_status", F.lower(F.trim("order_status")))
            .withColumn("ingestion_date", F.lit(ingestion_date).cast("date")))


def manifest_row_count(ingestion_date: str) -> int:
    path = config.RAW_DIR / "_manifests" / f"ingestion_date={ingestion_date}" / "manifest.json"
    entries = json.loads(path.read_text())["datasets"]
    return next(e["row_count"] for e in entries if e["dataset"] == DATASET)


def run(ingestion_date: str | None = None, spark: SparkSession | None = None) -> None:
    owns_spark = spark is None
    spark = spark or get_spark("transform-orders")
    try:
        ingestion_date = ingestion_date or latest_completed_date(config.RAW_DIR)

        raw = read_raw_orders(spark, ingestion_date)
        df = clean_orders(raw, ingestion_date)

        # Reconcile against the manifest written at ingestion time.
        expected, actual = manifest_row_count(ingestion_date), df.count()
        if actual != expected:
            raise ValueError(f"orders: read {actual} rows, manifest says {expected}")

        (df.coalesce(1)
           .write.mode("overwrite")
           .partitionBy("ingestion_date")
           .parquet(str(config.PROCESSED_DIR / DATASET)))
        log.info("orders: wrote %d rows for ingestion_date=%s", actual, ingestion_date)
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="ingestion date; default: latest completed")
    run(parser.parse_args().date)