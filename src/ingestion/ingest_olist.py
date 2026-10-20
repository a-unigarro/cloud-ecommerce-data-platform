"""Copy Olist CSVs from landing into the raw layer, unmodified, and write a manifest.

The raw layer is a local folder or an S3 location (RAW_DIR=s3://bucket/prefix):
    <raw>/<dataset>/ingestion_date=YYYY-MM-DD/<file>.csv
    <raw>/_manifests/ingestion_date=YYYY-MM-DD/manifest.json
"""
import argparse
import csv
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from src import config, storage

log = logging.getLogger("ingest_olist")


def profile_csv(path: Path) -> tuple[list[str], int]:
    """Return (header columns, number of data records).

    We use the csv module instead of counting lines because review comments
    contain line breaks inside quoted fields, so lines != rows.
    """
    with path.open(newline="", encoding="utf-8") as f:
        ## Convert the information into an iterator object that handles line breaks inside quoted fields.
        reader = csv.reader(f)
        header = next(reader)
        return header, sum(1 for _ in reader)



def ingest_dataset(name: str, filename: str, ingestion_date: str) -> dict:
    src = config.LANDING_DIR / filename          # landing is always a local folder
    relative = f"{name}/ingestion_date={ingestion_date}/{filename}"

    sha256 = storage.put_file(src, storage.join(config.RAW_DIR, relative))
    columns, row_count = profile_csv(src)        # the copy is verified identical, so profile the source

    return {
        "dataset": name,
        "source_file": filename,
        "raw_path": relative,
        "ingestion_date": ingestion_date,
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "row_count": row_count,
        "columns": columns,
        "size_bytes": src.stat().st_size,
        "sha256": sha256,
    }



def write_manifest(entries: list[dict], ingestion_date: str):
    dest = storage.join(config.RAW_DIR, "_manifests", f"ingestion_date={ingestion_date}", "manifest.json")
    payload = json.dumps({"ingestion_date": ingestion_date, "datasets": entries}, indent=2)
    storage.put_bytes(payload.encode(), dest)
    return dest



def run(ingestion_date: str):
    entries = []
    for name, filename in config.DATASETS.items():  # core: missing file = hard failure
        if not (config.LANDING_DIR / filename).exists():
            raise FileNotFoundError(f"Required dataset '{name}' missing: {filename}")
        entries.append(ingest_dataset(name, filename, ingestion_date))
        log.info("ingested %-22s %9d rows", name, entries[-1]["row_count"])

    for name, filename in config.OPTIONAL_DATASETS.items():  # optional: skip with a warning
        if (config.LANDING_DIR / filename).exists():
            entries.append(ingest_dataset(name, filename, ingestion_date))
            log.info("ingested %-22s %9d rows", name, entries[-1]["row_count"])
        else:
            log.warning("optional dataset '%s' not found, skipping", name)

    manifest = write_manifest(entries, ingestion_date)   # last = completion marker
    log.info("manifest written: %s", manifest)
    return manifest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="ingestion date, YYYY-MM-DD (default: today)")
    run(parser.parse_args().date)