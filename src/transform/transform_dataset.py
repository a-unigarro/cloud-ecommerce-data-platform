"""Raw CSV -> processed Parquet, for any Olist dataset described in SPECS.

Processed layout (mirrors raw / future S3):
    processed/olist/<dataset>/ingestion_date=YYYY-MM-DD/part-*.parquet
"""
import argparse
import json
import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

from src import config
from src.transform import schemas
from src.transform.raw_reader import latest_completed_date
from src.transform.session import get_spark

log = logging.getLogger("transform_dataset")
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


@dataclass(frozen=True)
class Spec:
    """Everything that differs between datasets. The pipeline code stays generic."""
    schema: StructType
    read_options: dict = field(default_factory=dict)
    lower: tuple = ()      # columns to lowercase
    upper: tuple = ()      # columns to uppercase
    rename: dict = field(default_factory=dict)   # old name -> new name


SPECS = {
    "orders": Spec(schemas.ORDERS_SCHEMA, lower=("order_status",)),
    "customers": Spec(schemas.CUSTOMERS_SCHEMA,
                      lower=("customer_city",), upper=("customer_state",)),
    "order_items": Spec(schemas.ORDER_ITEMS_SCHEMA),
    "payments": Spec(schemas.PAYMENTS_SCHEMA, lower=("payment_type",)),
    # Review comments contain line breaks and quotes inside quoted fields.
    "reviews": Spec(schemas.REVIEWS_SCHEMA,
                    read_options={"multiLine": True, "quote": '"', "escape": '"'}),
    "products": Spec(schemas.PRODUCTS_SCHEMA,
                     rename={"product_name_lenght": "product_name_length",
                             "product_description_lenght": "product_description_length"}),
    "sellers": Spec(schemas.SELLERS_SCHEMA,
                    lower=("seller_city",), upper=("seller_state",)),
    "category_translation": Spec(schemas.CATEGORY_TRANSLATION_SCHEMA),
}


def read_raw(spark: SparkSession, name: str, ingestion_date: str) -> DataFrame:
    spec = SPECS[name]
    path = config.RAW_DIR / name / f"ingestion_date={ingestion_date}"
    return (spark.read
            .option("header", True)
            .option("enforceSchema", False)   # header names must match the schema, not just positions
            .option("mode", "FAILFAST")       # a value that doesn't fit the schema = error
            .option("timestampFormat", TIMESTAMP_FORMAT)
            .options(**spec.read_options)
            .schema(spec.schema)
            .csv(str(path)))


def clean_dataset(name: str, df: DataFrame, ingestion_date: str) -> DataFrame:
    """Standardize values. Never drops rows: nulls are real information."""
    spec = SPECS[name]
    for f in df.schema.fields:
        if isinstance(f.dataType, StringType):
            trimmed = F.trim(F.col(f.name))
            # trimmed value if non-empty, otherwise null (when() without otherwise gives null)
            df = df.withColumn(f.name, F.when(trimmed != "", trimmed))
    for col in spec.lower:
        df = df.withColumn(col, F.lower(F.col(col)))
    for col in spec.upper:
        df = df.withColumn(col, F.upper(F.col(col)))
    if spec.rename:
        df = df.withColumnsRenamed(spec.rename)
    return df.withColumn("ingestion_date", F.lit(ingestion_date).cast("date"))


def manifest_row_count(name: str, ingestion_date: str) -> int:
    path = config.RAW_DIR / "_manifests" / f"ingestion_date={ingestion_date}" / "manifest.json"
    entries = json.loads(path.read_text())["datasets"]
    return next(e["row_count"] for e in entries if e["dataset"] == name)


def run(name: str, ingestion_date: str | None = None, spark: SparkSession | None = None) -> None:
    owns_spark = spark is None
    spark = spark or get_spark(f"transform-{name}")
    try:
        ingestion_date = ingestion_date or latest_completed_date(config.RAW_DIR)
        df = clean_dataset(name, read_raw(spark, name, ingestion_date), ingestion_date)

        expected, actual = manifest_row_count(name, ingestion_date), df.count()
        if actual != expected:
            raise ValueError(f"{name}: read {actual} rows, manifest says {expected}")

        (df.coalesce(1)
           .write.mode("overwrite")
           .partitionBy("ingestion_date")
           .parquet(str(config.PROCESSED_DIR / name)))
        log.info("%-22s wrote %9d rows for ingestion_date=%s", name, actual, ingestion_date)
    finally:
        if owns_spark:
            spark.stop()


def run_all(ingestion_date: str | None = None, spark: SparkSession | None = None) -> None:
    owns_spark = spark is None
    spark = spark or get_spark("transform-all")
    try:
        ingestion_date = ingestion_date or latest_completed_date(config.RAW_DIR)
        for name in SPECS:
            run(name, ingestion_date, spark=spark)
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=sorted(SPECS), default=None,
                        help="one dataset; default: all")
    parser.add_argument("--date", default=None, help="ingestion date; default: latest completed")
    args = parser.parse_args()
    run(args.dataset, args.date) if args.dataset else run_all(args.date)