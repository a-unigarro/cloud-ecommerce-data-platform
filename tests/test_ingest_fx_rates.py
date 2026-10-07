import json
from datetime import date, timedelta

import pytest
import requests

from src import config
from src.ingestion import ingest_fx_rates as fx

DATE = "2026-01-15"


def make_body(start: date, end: date) -> bytes:
    """Fake v2 response: one record per day per currency. indent=2 on purpose,
    so re-serialized JSON would differ from this and the 'untouched' test can tell."""
    records, day = [], start
    while day <= end:
        for quote, rate in (("EUR", 0.27), ("USD", 0.31)):
            records.append({"date": day.isoformat(), "base": "BRL", "quote": quote, "rate": rate})
        day += timedelta(days=1)
    return json.dumps(records, indent=2).encode()


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Temp raw folder, a 2-chunk date range (Sep 15-30 and Oct 1-10), no network."""
    raw = tmp_path / "frankfurter"
    monkeypatch.setattr(config, "RAW_API_DIR", raw)
    monkeypatch.setattr(config, "FX_START_DATE", "2016-09-15")
    monkeypatch.setattr(config, "FX_END_DATE", "2016-10-10")

    served = {}  # month -> exact bytes the fake API returned

    def fake_fetch(session, start, end):
        body = make_body(start, end)
        served[f"{start:%Y-%m}"] = body
        return f"https://fake/rates?from={start}&to={end}", body

    monkeypatch.setattr(fx, "fetch_chunk", fake_fetch)
    return raw, served


def partition(raw, ingestion_date=DATE):
    return raw / "exchange_rates" / f"ingestion_date={ingestion_date}"


def load_manifest(raw):
    path = raw / "_manifests" / f"ingestion_date={DATE}" / "manifest.json"
    return json.loads(path.read_text())


# --- pure function ---------------------------------------------------------

def test_month_chunks_handles_partial_months_and_leap_year():
    assert fx.month_chunks(date(2016, 9, 15), date(2016, 10, 10)) == [
        (date(2016, 9, 15), date(2016, 9, 30)),
        (date(2016, 10, 1), date(2016, 10, 10)),
    ]
    assert fx.month_chunks(date(2016, 2, 1), date(2016, 2, 29)) == [(date(2016, 2, 1), date(2016, 2, 29))]


def test_month_chunks_full_olist_range_is_26_months():
    assert len(fx.month_chunks(date(2016, 9, 1), date(2018, 10, 31))) == 26


# --- the HTTP call (v2 contract) -------------------------------------------

def test_fetch_chunk_uses_v2_url_and_params():
    class FakeResponse:
        url, content = "https://fake", b"[]"
        def raise_for_status(self): pass

    class FakeSession:
        def get(self, url, params, timeout):
            self.call = (url, params, timeout)
            return FakeResponse()

    session = FakeSession()
    fx.fetch_chunk(session, date(2016, 9, 1), date(2016, 9, 30))
    url, params, timeout = session.call

    assert url == f"{config.FX_BASE_URL}/rates"
    assert params == {"from": "2016-09-01", "to": "2016-09-30", "base": "BRL", "quotes": "EUR,USD"}
    assert timeout is not None  # never call an API without a timeout


# --- ingestion -------------------------------------------------------------

def test_creates_one_file_per_month_in_hive_layout(env):
    raw, _ = env
    fx.run(DATE)
    assert sorted(p.name for p in partition(raw).iterdir()) == ["rates_2016-09.json", "rates_2016-10.json"]


def test_raw_file_is_exactly_what_the_api_returned(env):
    raw, served = env
    fx.run(DATE)
    assert (partition(raw) / "rates_2016-09.json").read_bytes() == served["2016-09"]
    assert (partition(raw) / "rates_2016-10.json").read_bytes() == served["2016-10"]


def test_manifest_records_records_and_distinct_days(env):
    raw, _ = env
    fx.run(DATE)
    entries = {e["period_start"]: e for e in load_manifest(raw)["datasets"]}

    sep, octo = entries["2016-09-15"], entries["2016-10-01"]
    assert (sep["row_count"], sep["distinct_dates"]) == (32, 16)   # 16 days x 2 currencies
    assert (octo["row_count"], octo["distinct_dates"]) == (20, 10)
    assert sep["raw_path"] == f"exchange_rates/ingestion_date={DATE}/rates_2016-09.json"
    assert octo["raw_path"] == f"exchange_rates/ingestion_date={DATE}/rates_2016-10.json"


def test_rerun_same_date_is_idempotent(env):
    raw, _ = env
    fx.run(DATE)
    fx.run(DATE)
    assert sorted(p.name for p in partition(raw).iterdir()) == ["rates_2016-09.json", "rates_2016-10.json"]
    assert len(load_manifest(raw)["datasets"]) == 2


@pytest.mark.parametrize("bad_body", [
    b'{"rates": {"2016-09-15": {"EUR": 0.27}}}',   # old v1 shape
    b"[]",                                          # empty
    b'[{"date": "2016-09-15"}]',                    # missing fields
])
def test_unexpected_response_is_rejected_and_nothing_is_written(env, monkeypatch, bad_body):
    raw, _ = env
    monkeypatch.setattr(fx, "fetch_chunk", lambda s, a, b: ("https://fake", bad_body))
    with pytest.raises(ValueError):
        fx.run(DATE)
    assert not partition(raw).exists()
    assert not (raw / "_manifests").exists()


def test_failure_midway_leaves_no_manifest(env, monkeypatch):
    raw, _ = env

    def flaky(session, start, end):
        if start.month == 10:
            raise requests.HTTPError("503 Service Unavailable")
        return "https://fake", make_body(start, end)

    monkeypatch.setattr(fx, "fetch_chunk", flaky)
    with pytest.raises(requests.HTTPError):
        fx.run(DATE)

    assert (partition(raw) / "rates_2016-09.json").exists()   # partial data may remain...
    assert not (raw / "_manifests").exists()                  # ...but no completion marker