import json
from datetime import datetime

import pytest

from src import config
from src.transform import transform_dataset as t
from src.transform.schemas import ORDERS_SCHEMA

HEADER = ("order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,"
          "order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date\n")
CSV_ROWS = (
    "o1,c1,delivered,2017-10-02 10:56:33,2017-10-02 11:07:15,2017-10-04 19:55:00,2017-10-10 21:25:13,2017-10-18 00:00:00\n"
    "o2,c2,canceled,2018-07-24 20:41:37,,,,2018-08-13 00:00:00\n"   # real nulls
)


# --- pure logic ------------------------------------------------------------

def test_clean_orders_standardizes_text_and_adds_ingestion_date(spark):
    ts = datetime(2017, 10, 2, 10, 56, 33)
    raw = spark.createDataFrame(
        [(" o1 ", " c1", " Delivered ", ts, None, None, None, ts)], ORDERS_SCHEMA)

    row = t.clean_dataset("orders", raw, "2026-01-15").collect()[0]

    assert row.order_id == "o1"
    assert row.customer_id == "c1"
    assert row.order_status == "delivered"
    assert str(row.ingestion_date) == "2026-01-15"


def test_clean_orders_never_drops_rows_or_fills_nulls(spark):
    ts = datetime(2017, 10, 2, 10, 56, 33)
    raw = spark.createDataFrame(
        [("o1", "c1", "canceled", ts, None, None, None, ts),
         ("o2", "c2", "shipped", ts, None, None, None, ts)], ORDERS_SCHEMA)

    out = t.clean_dataset("orders", raw, "2026-01-15")

    assert out.count() == 2
    assert out.filter("order_delivered_customer_date IS NOT NULL").count() == 0


# --- end to end on fake files ----------------------------------------------

@pytest.fixture
def dirs(tmp_path, monkeypatch):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    monkeypatch.setattr(config, "RAW_DIR", raw)
    monkeypatch.setattr(config, "PROCESSED_DIR", processed)
    return raw, processed


def make_raw(raw, date, csv_rows=CSV_ROWS, manifest_rows=2):
    folder = raw / "orders" / f"ingestion_date={date}"
    folder.mkdir(parents=True)
    (folder / "orders.csv").write_text(HEADER + csv_rows)
    manifest_dir = raw / "_manifests" / f"ingestion_date={date}"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "manifest.json").write_text(
        json.dumps({"datasets": [{"dataset": "orders", "row_count": manifest_rows}]}))


def read_processed(spark, processed):
    return spark.read.parquet(str(processed / "orders"))


def test_run_writes_typed_parquet_partition(spark, dirs):
    raw, processed = dirs
    make_raw(raw, "2026-01-15")
    t.run("orders", "2026-01-15", spark=spark)

    assert (processed / "orders" / "ingestion_date=2026-01-15").exists()
    df = read_processed(spark, processed)
    assert df.count() == 2
    assert dict(df.dtypes)["order_purchase_timestamp"] == "timestamp"


def test_run_defaults_to_latest_completed_date(spark, dirs):
    raw, processed = dirs
    make_raw(raw, "2026-01-15")
    make_raw(raw, "2026-01-16")
    (raw / "orders" / "ingestion_date=2026-01-17").mkdir()   # no manifest: must be ignored
    t.run("orders", spark=spark)

    assert (processed / "orders" / "ingestion_date=2026-01-16").exists()
    assert not (processed / "orders" / "ingestion_date=2026-01-17").exists()


def test_rerun_same_date_does_not_duplicate(spark, dirs):
    raw, processed = dirs
    make_raw(raw, "2026-01-15")
    t.run("orders", "2026-01-15", spark=spark)
    t.run("orders", "2026-01-15", spark=spark)
    assert read_processed(spark, processed).count() == 2


def test_new_date_keeps_previous_partition(spark, dirs):
    raw, processed = dirs
    make_raw(raw, "2026-01-15")
    make_raw(raw, "2026-01-16")
    t.run("orders", "2026-01-15", spark=spark)
    t.run("orders", "2026-01-16", spark=spark)


    df = read_processed(spark, processed)
    assert df.count() == 4
    assert df.select("ingestion_date").distinct().count() == 2


def test_row_count_mismatch_with_manifest_fails_and_writes_nothing(spark, dirs):
    raw, processed = dirs
    make_raw(raw, "2026-01-15", manifest_rows=5)   # manifest claims 5, file has 2
    # The error should be a ValueError type and contain the word "manifest" in the message.
    with pytest.raises(ValueError, match="manifest"):
        t.run("orders", "2026-01-15", spark=spark)
    assert not (processed / "orders").exists()