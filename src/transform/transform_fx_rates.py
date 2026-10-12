"""Raw FX JSON (Frankfurter v2) -> processed Parquet.

Processed layout:
    processed/frankfurter/exchange_rates/ingestion_date=YYYY-MM-DD/part-*.parquet
    processed/frankfurter/_manifests/ingestion_date=YYYY-MM-DD/manifest.json
"""
import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from src import config
from src.transform.raw_reader import latest_completed_date
from src.transform.session import get_spark
from src.transform.transform_dataset import write_processed_manifest

log = logging.getLogger("transform_fx_rates")
DATASET = "exchange_rates"
KEY_COLUMNS = ["rate_date", "base", "quote", "rate"]

FX_SCHEMA = StructType([
    StructField("date", DateType(), True),
    StructField("base", StringType(), True),
    StructField("quote", StringType(), True),
    StructField("rate", DecimalType(18, 8), True),   # exact decimals, not floats
])


def read_raw_fx(spark: SparkSession, ingestion_date: str) -> DataFrame:
    path = config.RAW_API_DIR / DATASET / f"ingestion_date={ingestion_date}"
    return (spark.read
            .option("multiLine", True)       # each file is one JSON array spanning many lines
            .option("mode", "FAILFAST")
            .schema(FX_SCHEMA)
            .json(str(path)))


def clean_fx(df: DataFrame, ingestion_date: str) -> DataFrame:
    return (df
            .withColumnRenamed("date", "rate_date")
            .withColumn("base", F.upper(F.trim("base")))
            .withColumn("quote", F.upper(F.trim("quote")))
            .withColumn("ingestion_date", F.lit(ingestion_date).cast("date")))


def manifest_row_count(ingestion_date: str) -> int:
    path = config.RAW_API_DIR / "_manifests" / f"ingestion_date={ingestion_date}" / "manifest.json"
    entries = json.loads(path.read_text())["datasets"]
    return sum(e["row_count"] for e in entries if e["dataset"] == DATASET)


def run(ingestion_date: str | None = None, spark: SparkSession | None = None) -> Path:
    owns_spark = spark is None
    spark = spark or get_spark("transform-fx-rates")
    try:
        ingestion_date = ingestion_date or latest_completed_date(config.RAW_API_DIR)
        df = clean_fx(read_raw_fx(spark, ingestion_date), ingestion_date).cache()

        # JSON with a schema fills missing fields with null instead of failing: check explicitly.
        for col in KEY_COLUMNS:
            if df.filter(F.col(col).isNull()).limit(1).count():
                raise ValueError(f"{DATASET}: null values in required column '{col}'")

        expected, actual = manifest_row_count(ingestion_date), df.count()
        if actual != expected:
            raise ValueError(f"{DATASET}: read {actual} rows, manifest says {expected}")

        (df.coalesce(1)
           .write.mode("overwrite")
           .partitionBy("ingestion_date")
           .parquet(str(config.PROCESSED_API_DIR / DATASET)))
        log.info("%s wrote %d rows for ingestion_date=%s", DATASET, actual, ingestion_date)

        partition = config.PROCESSED_API_DIR / DATASET / f"ingestion_date={ingestion_date}"
        entry = {
            "dataset": DATASET,
            "source_ingestion_date": ingestion_date,
            "transformed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "row_count": actual,
            "columns": [{"name": n, "type": t} for n, t in df.dtypes],
            "files": [{"path": str(f.relative_to(config.PROCESSED_API_DIR)),
                       "size_bytes": f.stat().st_size}
                      for f in sorted(partition.glob("*.parquet"))],
        }
        return write_processed_manifest([entry], ingestion_date, base_dir=config.PROCESSED_API_DIR)
    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="ingestion date; default: latest completed")
    run(parser.parse_args().date)