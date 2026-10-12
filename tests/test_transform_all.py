import json
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from src import config
from src.transform import schemas
from src.transform import transform_dataset as t

DATE = "2026-01-15"


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    raw, processed = tmp_path / "raw", tmp_path / "processed"
    monkeypatch.setattr(config, "RAW_DIR", raw)
    monkeypatch.setattr(config, "PROCESSED_DIR", processed)
    return raw, processed


def write_raw(raw, name, rows, header=None, date=DATE):
    """Write a fake raw CSV. Default header = the schema's column names."""
    cols = header or [f.name for f in t.SPECS[name].schema.fields]
    folder = raw / name / f"ingestion_date={date}"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{name}.csv").write_text(",".join(cols) + "\n" + "".join(rows))


def write_raw_manifest(raw, counts, date=DATE):
    folder = raw / "_manifests" / f"ingestion_date={date}"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "manifest.json").write_text(json.dumps(
        {"datasets": [{"dataset": n, "row_count": c} for n, c in counts.items()]}))


def blank_row(name):
    """A row of empty values: valid CSV, every field null."""
    return ",".join([""] * len(t.SPECS[name].schema.fields)) + "\n"


def make_all_raw(raw, overrides=None):
    """One blank row for every dataset, plus a matching raw manifest."""
    counts = {name: 1 for name in t.SPECS} | (overrides or {})
    for name in t.SPECS:
        write_raw(raw, name, [blank_row(name)])
    write_raw_manifest(raw, counts)


# --- generic cleaning rules ------------------------------------------------

def test_customers_text_rules_and_zip_keeps_leading_zero(spark):
    df = spark.createDataFrame(
        [("c1", "u1", "01001", " Sao Paulo ", "sp"),
         ("c2", "u2", None, "   ", "RJ")],
        schemas.CUSTOMERS_SCHEMA)

    rows = {r.customer_id: r for r in t.clean_dataset("customers", df, DATE).collect()}

    assert rows["c1"].customer_city == "sao paulo"      # trimmed + lowercased
    assert rows["c1"].customer_state == "SP"            # uppercased
    assert rows["c1"].customer_zip_code_prefix == "01001"
    assert rows["c2"].customer_city is None             # whitespace-only -> null


def test_products_columns_are_renamed_and_raw_typo_is_gone(spark):
    df = spark.createDataFrame([], schemas.PRODUCTS_SCHEMA)
    cols = t.clean_dataset("products", df, DATE).columns

    assert "product_name_length" in cols and "product_description_length" in cols
    assert not any("lenght" in c for c in cols)
    assert cols[-1] == "ingestion_date"


# --- reading raw files -----------------------------------------------------

def test_reviews_multiline_comment_stays_one_row(spark, dirs):
    raw, _ = dirs
    write_raw(raw, "reviews", [
        'r1,o1,5,,"line one\nline two ""quoted""",2018-01-01 00:00:00,2018-01-02 10:00:00\n',
        'r2,o2,1,Bad,plain,2018-01-03 00:00:00,2018-01-04 00:00:00\n',
    ])
    df = t.clean_dataset("reviews", t.read_raw(spark, "reviews", DATE), DATE)

    rows = {r.review_id: r for r in df.collect()}
    assert df.count() == 2                               # not 3
    assert rows["r1"].review_comment_message == 'line one\nline two "quoted"'
    assert rows["r1"].review_comment_title is None       # empty -> null
    assert rows["r1"].review_score == 5


def test_payment_values_are_exact_decimals(spark, dirs):
    raw, _ = dirs
    write_raw(raw, "payments", ["o1,1,credit_card,1,0.10\n", "o2,1,credit_card,1,0.20\n"])
    df = t.read_raw(spark, "payments", DATE)

    total = df.agg(F.sum("payment_value")).collect()[0][0]
    assert total == Decimal("0.30")       # with floats, 0.1 + 0.2 != 0.3


def test_header_that_does_not_match_schema_is_rejected(spark, dirs):
    raw, _ = dirs
    # Same number of columns, but two names swapped: positional reading would accept this.
    write_raw(raw, "customers", ["c1,u1,01001,sao paulo,SP\n"],
              header=["customer_unique_id", "customer_id", "customer_zip_code_prefix",
                      "customer_city", "customer_state"])
    with pytest.raises(Exception, match="(?i)header"):
        t.read_raw(spark, "customers", DATE).collect()


def test_value_that_does_not_fit_type_is_rejected(spark, dirs):
    raw, _ = dirs
    write_raw(raw, "payments", ["o1,1,credit_card,1,not_a_number\n"])
    with pytest.raises(Exception):
        t.read_raw(spark, "payments", DATE).collect()


# --- processed manifest (completion marker) --------------------------------

def test_run_all_writes_every_dataset_and_a_complete_manifest(spark, dirs):
    raw, processed = dirs
    make_all_raw(raw)
    manifest_path = t.run_all(DATE, spark=spark)

    manifest = json.loads(manifest_path.read_text())
    entries = {e["dataset"]: e for e in manifest["datasets"]}
    # with sets, the order doesn't matter, compare the names only
    assert set(entries) == set(t.SPECS)
    for name, entry in entries.items():
        assert entry["row_count"] == 1
        assert entry["columns"][-1] == {"name": "ingestion_date", "type": "date"}
        assert len(entry["files"]) == 1 and entry["files"][0]["size_bytes"] > 0
        assert (processed / name / f"ingestion_date={DATE}").exists()


def test_run_all_failure_leaves_no_manifest(spark, dirs):
    raw, processed = dirs
    make_all_raw(raw, overrides={"sellers": 5})          # raw manifest lies about sellers
    with pytest.raises(ValueError, match="sellers"):
        t.run_all(DATE, spark=spark)
    assert not (processed / "_manifests").exists()       # day is NOT marked complete


def test_single_dataset_run_does_not_write_a_manifest(spark, dirs):
    raw, processed = dirs
    make_all_raw(raw)
    entry = t.run("orders", DATE, spark=spark)
    assert entry["dataset"] == "orders" and entry["row_count"] == 1
    assert not (processed / "_manifests").exists()


def test_latest_completed_date_works_on_processed_layer(spark, dirs):
    from src.transform.raw_reader import latest_completed_date
    raw, processed = dirs
    make_all_raw(raw)
    t.run_all(DATE, spark=spark)
    assert latest_completed_date(processed) == DATE