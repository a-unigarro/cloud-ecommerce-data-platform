import hashlib
import json

import pytest

from src import config
from src.ingestion import ingest_olist

DATE = "2026-01-15"


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Tiny fake landing area, with config redirected to temp folders."""
    landing, raw = tmp_path / "landing", tmp_path / "raw"
    landing.mkdir()

    (landing / "orders.csv").write_text("order_id,status\n1,delivered\n2,shipped\n3,canceled\n")
    # One record containing a line break inside quotes: 2 rows, but 4 physical lines.
    (landing / "reviews.csv").write_text('review_id,comment\n1,"line one\nline two"\n2,plain\n')
    (landing / "geo.csv").write_text("zip,lat\n111,1.0\n")
    ## override config values to point to our temp dirs and test files
    monkeypatch.setattr(config, "LANDING_DIR", landing)
    monkeypatch.setattr(config, "RAW_DIR", raw)
    monkeypatch.setattr(config, "DATASETS", {"orders": "orders.csv", "reviews": "reviews.csv"})
    monkeypatch.setattr(config, "OPTIONAL_DATASETS", {"geolocation": "geo.csv"})
    return landing, raw


def load_manifest(raw):
    # regular function for loading the manifest
    path = raw / "_manifests" / f"ingestion_date={DATE}" / "manifest.json"
    return json.loads(path.read_text())


def test_creates_hive_partitioned_layout(env):
    # check the existence of the expected files and directories after running the ingestion
    _, raw = env
    ingest_olist.run(DATE)
    assert (raw / "orders" / f"ingestion_date={DATE}" / "orders.csv").exists()
    assert (raw / "reviews" / f"ingestion_date={DATE}" / "reviews.csv").exists()


def test_raw_file_is_byte_identical_to_source(env):
    # check the byte-level identity of the copied file to the source file after ingestion
    landing, raw = env
    ingest_olist.run(DATE)
    copied = raw / "orders" / f"ingestion_date={DATE}" / "orders.csv"
    assert copied.read_bytes() == (landing / "orders.csv").read_bytes()


def test_manifest_records_rows_columns_and_checksum(env):
    # check that the manifest correctly records the number of rows, columns, and checksum of the ingested files
    landing, raw = env
    ingest_olist.run(DATE)
    entries = {e["dataset"]: e for e in load_manifest(raw)["datasets"]}

    assert entries["orders"]["row_count"] == 3
    assert entries["orders"]["columns"] == ["order_id", "status"]
    expected = hashlib.sha256((landing / "orders.csv").read_bytes()).hexdigest()
    assert entries["orders"]["sha256"] == expected


def test_row_count_handles_multiline_quoted_fields(env):
    _, raw = env
    ingest_olist.run(DATE)
    entries = {e["dataset"]: e for e in load_manifest(raw)["datasets"]}
    assert entries["reviews"]["row_count"] == 2  # not 4


def test_rerun_same_date_is_idempotent(env):    
    _, raw = env
    ingest_olist.run(DATE)
    first = load_manifest(raw)
    ingest_olist.run(DATE)
    second = load_manifest(raw)

    partition = raw / "orders" / f"ingestion_date={DATE}"
    assert [p.name for p in partition.iterdir()] == ["orders.csv"]  # no duplicates, no .tmp
    assert len(second["datasets"]) == len(first["datasets"])
    assert [e["sha256"] for e in second["datasets"]] == [e["sha256"] for e in first["datasets"]]


def test_new_date_creates_new_partition_and_keeps_old(env):
    _, raw = env
    ingest_olist.run("2026-01-15")
    ingest_olist.run("2026-01-16")
    assert (raw / "orders" / "ingestion_date=2026-01-15" / "orders.csv").exists()
    assert (raw / "orders" / "ingestion_date=2026-01-16" / "orders.csv").exists()


def test_missing_core_dataset_fails_and_writes_no_manifest(env):
    landing, raw = env
    (landing / "reviews.csv").unlink()
    #test the expected exception
    with pytest.raises(FileNotFoundError):
        ingest_olist.run(DATE)
    assert not (raw / "_manifests").exists()


def test_missing_optional_dataset_is_skipped(env):
    landing, raw = env
    (landing / "geo.csv").unlink()
    ingest_olist.run(DATE)
    names = {e["dataset"] for e in load_manifest(raw)["datasets"]}
    assert names == {"orders", "reviews"}