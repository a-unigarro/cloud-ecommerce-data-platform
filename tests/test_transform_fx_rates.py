import json
from datetime import date
from decimal import Decimal

import pytest

from src import config
from src.transform import transform_fx_rates as fx

DATE = "2026-01-15"


def rec(day, quote="EUR", rate=0.27, base="BRL"):
    return {"date": day, "base": base, "quote": quote, "rate": rate}


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    monkeypatch.setattr(config, "RAW_API_DIR", raw)
    monkeypatch.setattr(config, "PROCESSED_API_DIR", processed)
    return raw, processed


def write_raw(raw, files, manifest_rows=None, ingestion_date=DATE):
    """files: {filename: [records]}. Written pretty-printed, like a real multi-line API response."""
    folder = raw / "exchange_rates" / f"ingestion_date={ingestion_date}"
    folder.mkdir(parents=True, exist_ok=True)
    for filename, records in files.items():
        (folder / filename).write_text(json.dumps(records, indent=2))
    total = sum(len(r) for r in files.values()) if manifest_rows is None else manifest_rows
    manifest_dir = raw / "_manifests" / f"ingestion_date={ingestion_date}"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "manifest.json").write_text(json.dumps(
        {"datasets": [{"dataset": "exchange_rates", "row_count": total}]}))


# --- pure logic ------------------------------------------------------------

def test_clean_fx_renames_date_and_standardizes_codes(spark):
    df = spark.createDataFrame(
        [(date(2016, 9, 1), " brl ", "eur", Decimal("0.27698"))], fx.FX_SCHEMA)

    out = fx.clean_fx(df, DATE)
    row = out.collect()[0]

    assert "date" not in out.columns and "rate_date" in out.columns
    assert (row.base, row.quote) == ("BRL", "EUR")
    assert str(row.ingestion_date) == DATE
    assert out.columns[-1] == "ingestion_date"


# --- end to end ------------------------------------------------------------

def test_run_writes_typed_parquet_and_manifest(spark, dirs):
    raw, processed = dirs
    write_raw(raw, {"rates_2016-09.json": [
        rec("2016-09-01", "EUR", 0.27698), rec("2016-09-01", "USD", 0.30904)]})

    manifest_path = fx.run(DATE, spark=spark)

    df = spark.read.parquet(str(processed / "exchange_rates"))
    types = dict(df.dtypes)
    assert df.count() == 2
    assert types["rate_date"] == "date" and types["rate"] == "decimal(18,8)"
    assert df.filter("quote = 'EUR'").collect()[0].rate == Decimal("0.27698")

    manifest = json.loads(manifest_path.read_text())
    assert manifest_path == processed / "_manifests" / f"ingestion_date={DATE}" / "manifest.json"
    entry = manifest["datasets"][0]
    assert entry["dataset"] == "exchange_rates" and entry["row_count"] == 2
    assert len(entry["files"]) == 1


def test_multiple_monthly_files_are_all_read(spark, dirs):
    raw, processed = dirs
    write_raw(raw, {
        "rates_2016-09.json": [rec("2016-09-30", "EUR"), rec("2016-09-30", "USD")],
        "rates_2016-10.json": [rec("2016-10-01", "EUR"), rec("2016-10-01", "USD")],
    })
    fx.run(DATE, spark=spark)

    df = spark.read.parquet(str(processed / "exchange_rates"))
    assert df.count() == 4
    assert df.select("rate_date").distinct().count() == 2


def test_missing_required_field_is_rejected_and_nothing_is_written(spark, dirs):
    raw, processed = dirs
    broken = {"date": "2016-09-01", "base": "BRL", "quote": "EUR"}   # no "rate"
    write_raw(raw, {"rates_2016-09.json": [rec("2016-09-01", "USD"), broken]})

    with pytest.raises(ValueError, match="null values.*rate"):
        fx.run(DATE, spark=spark)

    assert not (processed / "exchange_rates").exists()
    assert not (processed / "_manifests").exists()


def test_row_count_mismatch_with_raw_manifest_fails_and_writes_nothing(spark, dirs):
    raw, processed = dirs
    write_raw(raw, {"rates_2016-09.json": [rec("2016-09-01")]}, manifest_rows=7)

    with pytest.raises(ValueError, match="manifest"):
        fx.run(DATE, spark=spark)

    assert not (processed / "exchange_rates").exists()
    assert not (processed / "_manifests").exists()


def test_rerun_same_date_does_not_duplicate(spark, dirs):
    raw, processed = dirs
    write_raw(raw, {"rates_2016-09.json": [rec("2016-09-01", "EUR"), rec("2016-09-01", "USD")]})
    fx.run(DATE, spark=spark)
    fx.run(DATE, spark=spark)

    assert spark.read.parquet(str(processed / "exchange_rates")).count() == 2