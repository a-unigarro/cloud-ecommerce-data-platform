"""Fetch BRL exchange rates from the Frankfurter API into the raw layer, unmodified.

Raw layout:
    raw/frankfurter/exchange_rates/ingestion_date=YYYY-MM-DD/rates_YYYY-MM.json
    raw/frankfurter/_manifests/ingestion_date=YYYY-MM-DD/manifest.json
"""
import argparse
import calendar
import hashlib
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tracemalloc import start


import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src import config

log = logging.getLogger("ingest_fx_rates")
DATASET = "exchange_rates"



def month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-month ranges (first and last day inclusive)."""
    chunks, current = [], start.replace(day=1) # (e.g 2024-06-15 --> 2024-06-01)
    while current <= end:
        last = current.replace(day=calendar.monthrange(current.year, current.month)[1]) # (e.g 2024-06-15 --> 2024-06-30)
        chunks.append((max(current, start), min(last, end)))
        current = last + timedelta(days=1)
    return chunks


def make_session() -> requests.Session:
    """Session that retries transient failures (rate limit, 5xx) with growing delays."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_chunk(session: requests.Session, start: date, end: date) -> tuple[str, bytes]:
    """One HTTP call. Kept separate so tests can replace it without any network."""
    params = {
        "from": start.isoformat(),
        "to": end.isoformat(),
        "base": config.FX_BASE_CURRENCY,
        "quotes": ",".join(config.FX_QUOTES),
    }
    resp = session.get(f"{config.FX_BASE_URL}/rates", params=params, timeout=30)
    resp.raise_for_status()
    return resp.url, resp.content


def ingest_chunk(session, start: date, end: date, ingestion_date: str) -> dict:
    request_url, body = fetch_chunk(session, start, end)
    payload = json.loads(body)  # validate only; we store the original bytes
    required = {"date", "base", "quote", "rate"}
    if (not isinstance(payload, list) or not payload
            or not all(isinstance(r, dict) and required <= r.keys() for r in payload)):
        raise ValueError(f"Unexpected or empty response for {start}..{end}: {request_url}")

    dest_dir = config.RAW_API_DIR / DATASET / f"ingestion_date={ingestion_date}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"rates_{start:%Y-%m}.json"
    tmp = dest.with_suffix(".json.tmp")
    try:
        tmp.write_bytes(body)
        os.replace(tmp, dest)  # atomic, overwrites on re-run -> idempotent
    finally:
        tmp.unlink(missing_ok=True)

    return {
        "dataset": DATASET,
        "request_url": request_url,
        "raw_path": str(dest.relative_to(config.RAW_API_DIR)),
        "ingestion_date": ingestion_date,
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "row_count": len(payload),
        "distinct_dates": len({r["date"] for r in payload}),  # one entry per day that has a rate
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


def write_manifest(entries: list[dict], ingestion_date: str) -> Path:
    manifest_dir = config.RAW_API_DIR / "_manifests" / f"ingestion_date={ingestion_date}"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_dir / "manifest.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"ingestion_date": ingestion_date, "datasets": entries}, indent=2))
    os.replace(tmp, path)
    return path


def run(ingestion_date: str) -> Path:
    start = date.fromisoformat(config.FX_START_DATE)
    end = date.fromisoformat(config.FX_END_DATE)
    session = make_session()

    entries = []
    for chunk_start, chunk_end in month_chunks(start, end):
        entries.append(ingest_chunk(session, chunk_start, chunk_end, ingestion_date))
        log.info("ingested %s..%s  %3d records, %2d days", chunk_start, chunk_end,
                 entries[-1]["row_count"], entries[-1]["distinct_dates"])

    manifest = write_manifest(entries, ingestion_date)  # last = completion marker
    log.info("manifest written: %s", manifest)
    return manifest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="ingestion date, YYYY-MM-DD (default: today)")
    run(parser.parse_args().date)
