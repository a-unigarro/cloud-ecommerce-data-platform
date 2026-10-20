from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def _location(env_var: str, default: Path) -> "str | Path":
    """A data location from the environment: an s3:// URI stays a string, anything else is a Path."""
    value = os.getenv(env_var)
    if not value:
        return default
    return value if value.startswith("s3://") else Path(value)

RAW_DIR = _location("RAW_DIR", PROJECT_ROOT / "data" / "raw" / "olist")
RAW_API_DIR = _location("RAW_API_DIR", PROJECT_ROOT / "data" / "raw" / "frankfurter")

LANDING_DIR = Path(os.getenv("LANDING_DIR", PROJECT_ROOT / "data" / "landing" / "olist"))


# Logical dataset name -> source filename.
# Logical names are what the rest of the pipeline uses; filenames are an
# implementation detail of how Olist happens to ship its files.
DATASETS = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# Optional enrichment, not required by the core pipeline.
OPTIONAL_DATASETS = {
    "geolocation": "olist_geolocation_dataset.csv",
}

FX_BASE_URL = "https://api.frankfurter.dev/v2"
FX_BASE_CURRENCY = "BRL"
FX_QUOTES = ["EUR", "USD"]
FX_START_DATE = "2016-09-01"   # Olist orders span roughly Sept 2016 - Oct 2018
FX_END_DATE = "2018-10-31"


PROCESSED_DIR = Path(os.getenv("PROCESSED_DIR", PROJECT_ROOT / "data" / "processed" / "olist"))

PROCESSED_API_DIR = Path(os.getenv("PROCESSED_API_DIR", PROJECT_ROOT / "data" / "processed" / "frankfurter"))