import hashlib
import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from src import config, storage
from src.ingestion import ingest_fx_rates as fx
from src.ingestion import ingest_olist

BUCKET, REGION, DATE = "test-bucket", "eu-central-1", "2026-01-15"


@pytest.fixture
def s3(monkeypatch):
    """A fake S3 in memory. Fake credentials guarantee nothing here can reach real AWS."""
    for var, value in {"AWS_ACCESS_KEY_ID": "testing", "AWS_SECRET_ACCESS_KEY": "testing",
                       "AWS_DEFAULT_REGION": REGION}.items():
        monkeypatch.setenv(var, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET,
                             CreateBucketConfiguration={"LocationConstraint": REGION})
        yield client


def keys(client, prefix=""):
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    return sorted(o["Key"] for page in pages for o in page.get("Contents", []))


def read(client, key):
    return client.get_object(Bucket=BUCKET, Key=key)["Body"].read()


# --- paths and primitives --------------------------------------------------

def test_is_s3_and_join():
    assert storage.is_s3("s3://b/p")
    assert not storage.is_s3(Path("/tmp/x")) and not storage.is_s3("/tmp/x")
    assert storage.join("s3://b/raw/", "orders", "f.csv") == "s3://b/raw/orders/f.csv"
    assert storage.join(Path("/data/raw"), "orders", "f.csv") == Path("/data/raw/orders/f.csv")


def test_put_bytes_local_creates_folders_and_leaves_no_tmp(tmp_path):
    dest = tmp_path / "a" / "b" / "file.json"
    storage.put_bytes(b'{"x": 1}', dest)
    assert dest.read_bytes() == b'{"x": 1}'
    # locate the parent folder and iterates over all the files in that folder, checking that the only file present is "file.json"
    assert [p.name for p in dest.parent.iterdir()] == ["file.json"]


def test_put_bytes_s3(s3):
    storage.put_bytes(b"hello", f"s3://{BUCKET}/raw/x.txt")
    assert read(s3, "raw/x.txt") == b"hello"


@pytest.mark.parametrize("target", ["local", "s3"])
def test_put_file_copies_exactly_and_returns_source_checksum(tmp_path, s3, target):
    src = tmp_path / "src.csv"
    src.write_bytes(b"a,b\n1,2\n" * 1000)
    dest = tmp_path / "out" / "src.csv" if target == "local" else f"s3://{BUCKET}/raw/src.csv"

    digest = storage.put_file(src, dest)

    assert digest == hashlib.sha256(src.read_bytes()).hexdigest()
    copied = dest.read_bytes() if target == "local" else read(s3, "raw/src.csv")
    assert copied == src.read_bytes()


# --- ingestion into S3 -----------------------------------------------------

@pytest.fixture
def landing(tmp_path, monkeypatch):
    folder = tmp_path / "landing"
    folder.mkdir()
    (folder / "orders.csv").write_text("order_id,status\n1,delivered\n2,shipped\n")
    (folder / "geo.csv").write_text("zip,lat\n111,1.0\n")
    monkeypatch.setattr(config, "LANDING_DIR", folder)
    monkeypatch.setattr(config, "DATASETS", {"orders": "orders.csv"})
    monkeypatch.setattr(config, "OPTIONAL_DATASETS", {"geolocation": "geo.csv"})
    monkeypatch.setattr(config, "RAW_DIR", f"s3://{BUCKET}/raw/olist")
    return folder


def test_olist_ingestion_to_s3_writes_data_and_manifest(s3, landing):
    manifest = ingest_olist.run(DATE)

    manifest_key = f"raw/olist/_manifests/ingestion_date={DATE}/manifest.json"
    assert manifest == f"s3://{BUCKET}/{manifest_key}"
    assert keys(s3) == [
        manifest_key,
        f"raw/olist/geolocation/ingestion_date={DATE}/geo.csv",
        f"raw/olist/orders/ingestion_date={DATE}/orders.csv",
    ]
    entries = {e["dataset"]: e for e in json.loads(read(s3, manifest_key))["datasets"]}
    assert entries["orders"]["row_count"] == 2
    assert entries["orders"]["raw_path"] == f"orders/ingestion_date={DATE}/orders.csv"


def test_olist_rerun_to_s3_does_not_duplicate(s3, landing):
    ingest_olist.run(DATE)
    first = keys(s3)
    ingest_olist.run(DATE)
    assert keys(s3) == first


def test_olist_missing_core_dataset_writes_no_manifest_to_s3(s3, landing):
    (landing / "orders.csv").unlink()
    with pytest.raises(FileNotFoundError):
        ingest_olist.run(DATE)
    assert keys(s3, "raw/olist/_manifests/") == []


def test_fx_ingestion_to_s3_keeps_response_bytes(s3, monkeypatch):
    monkeypatch.setattr(config, "RAW_API_DIR", f"s3://{BUCKET}/raw/frankfurter")
    monkeypatch.setattr(config, "FX_START_DATE", "2016-09-30")
    monkeypatch.setattr(config, "FX_END_DATE", "2016-10-01")
    body = json.dumps([{"date": "2016-09-30", "base": "BRL", "quote": "EUR", "rate": 0.27}],
                      indent=2).encode()
    # Replace the real fetch_chunk with a fake that returns our body and a fake URL.
    monkeypatch.setattr(fx, "fetch_chunk", lambda session, start, end: ("https://fake", body))

    fx.run(DATE)

    assert keys(s3) == [
        f"raw/frankfurter/_manifests/ingestion_date={DATE}/manifest.json",
        f"raw/frankfurter/exchange_rates/ingestion_date={DATE}/rates_2016-09.json",
        f"raw/frankfurter/exchange_rates/ingestion_date={DATE}/rates_2016-10.json",
    ]
    assert read(s3, f"raw/frankfurter/exchange_rates/ingestion_date={DATE}/rates_2016-09.json") == body