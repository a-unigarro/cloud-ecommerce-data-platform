from pathlib import Path


def latest_completed_date(raw_dir: Path) -> str:
    """Latest ingestion_date whose manifest exists (the completion marker)."""
    manifests = sorted((raw_dir / "_manifests").glob("ingestion_date=*/manifest.json"))
    if not manifests:
        raise FileNotFoundError(f"No completed ingestion found under {raw_dir}")
    return manifests[-1].parent.name.split("=")[1]   # ISO dates sort correctly as text