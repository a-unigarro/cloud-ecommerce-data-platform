"""Copy Olist CSVs from landing into the raw layer, unmodified, and write a manifest.
Raw layout (mirrors the future S3 layout):
    raw/olist/<dataset>/ingestion_date=YYYY-MM-DD/<file>.csv
    raw/olist/_manifests/ingestion_date=YYYY-MM-DD/manifest.json
"""
import argparse
import csv
import hashlib
import json
import logging
import os
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

from src import config

log = logging.getLogger("ingest_olist")


def sha256_of(path: Path) -> str:
    """Checksum read in chunks, so large files never load fully into memory."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
    src = config.LANDING_DIR / filename
    dest_dir = config.RAW_DIR / name / f"ingestion_date={ingestion_date}"
    dest = dest_dir / filename
    tmp = dest.with_suffix(dest.suffix + ".tmp") ### .csv --> .csv.tmp

    dest_dir.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(src, tmp)
        src_hash = sha256_of(src)
        if sha256_of(tmp) != src_hash:
            raise IOError(f"Checksum mismatch after copying {filename}")
        os.replace(tmp, dest)  # overwrites on re-run -> idempotent
    finally:
        tmp.unlink(missing_ok=True)

    columns, row_count = profile_csv(dest)
    return {
        "dataset": name,
        "source_file": filename,
        "raw_path": str(dest.relative_to(config.RAW_DIR)),
        "ingestion_date": ingestion_date,
        "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "row_count": row_count,
        "columns": columns,
        "size_bytes": dest.stat().st_size,
        "sha256": src_hash,
    }


def write_manifest(entries: list[dict], ingestion_date: str) -> Path:
    manifest_dir = config.RAW_DIR / "_manifests" / f"ingestion_date={ingestion_date}"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    path = manifest_dir / "manifest.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({"ingestion_date": ingestion_date, "datasets": entries}, indent=2))
    os.replace(tmp, path)
    return path


def run(ingestion_date: str) -> Path:
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

    manifest = write_manifest(entries, ingestion_date)
    log.info("manifest written: %s", manifest)
    return manifest


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="ingestion date, YYYY-MM-DD (default: today)")
    run(parser.parse_args().date)