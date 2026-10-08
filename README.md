# cloud-ecommerce-data-platform
 
A portfolio project that builds an end-to-end data platform for e-commerce data, developed progressively: locally first, with AWS introduced step by step. It uses the Olist Brazilian e-commerce dataset and a REST API as sources.
 
Planned stack: Python, PySpark, Airflow, AWS S3, Parquet, Athena, dbt, Terraform, Docker, GitHub Actions.
 
## Status

Milestone 1 (complete): local ingestion of the Olist CSV files and the Frankfurter exchange-rate API into a raw layer, with manifests and tests. Nothing runs on AWS yet.
Milestone 2 (in progress): PySpark transformations from raw CSV to processed Parquet, starting with orders.
 
## Architecture (target)
 
Olist CSVs + REST API -> Python ingestion -> S3 raw layer -> Airflow orchestration -> PySpark transformations -> S3 processed Parquet -> Athena -> dbt models -> analytics/reporting
 
## Getting the data
 

The data is not committed to the repository. To reproduce the setup:
 
1. Download the "Brazilian E-Commerce Public Dataset by Olist" from Kaggle.
2. Extract the CSV files into data/landing/olist/
3. Run the ingestion: python -m src.ingestion.ingest_olist
4. Fetch exchange rates (BRL to EUR/USD, no API key needed): python -m src.ingestion.ingest_fx_rates
5. Run the tests: pytest

Requires Python 3.11 and Java 17 (conda install -c conda-forge openjdk=17), plus the packages in requirements.txt.
 
## Data layers
 
- data/landing/olist/: the original CSV files, never modified.
- data/raw/olist/<dataset>/ingestion_date=YYYY-MM-DD/: an unmodified copy of each file, partitioned by ingestion date. This layout mirrors the future S3 layout.
- data/raw/olist/_manifests/ingestion_date=YYYY-MM-DD/manifest.json: row counts, columns, file sizes and SHA-256 checksums for each ingested file.
- data/raw/frankfurter/exchange_rates/ingestion_date=YYYY-MM-DD/: monthly JSON files, exactly as returned by the Frankfurter v2 API.
- data/raw/frankfurter/_manifests/...: manifest for the API ingestion, same convention as Olist.
 
## Design notes
 
- The raw layer is never cleaned or retyped; that happens later in PySpark.
- Ingestion is idempotent: re-running the same date overwrites that partition instead of duplicating data.
- The manifest is written only after all datasets are copied successfully, so it acts as a completion marker. Downstream steps should only process a date if its manifest exists.
- The API response is stored as the original bytes. It is validated before writing, so an unexpected response shape fails loudly instead of landing in raw.
- ingestion_date is when data was fetched, not the date of the data itself.
## Documentation
 
See docs/data_dictionary.md for a description of the Olist tables and their relationships.
