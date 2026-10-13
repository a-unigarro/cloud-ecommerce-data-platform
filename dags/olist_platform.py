"""Olist platform pipeline: ingest then transform, for both sources.

    resolve_date -> ingest_olist -> transform_olist
                 -> ingest_fx    -> transform_fx
"""
from datetime import date, datetime, timedelta, timezone

import pendulum
from airflow.sdk import Param, dag, task


@dag(
    dag_id="olist_platform",
    schedule=None,                       # manual trigger for now
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,                   # two runs would write the same partitions
    params={
        "ingestion_date": Param(
            "", type=["null", "string"], pattern=r"^(\d{4}-\d{2}-\d{2})?$",
            description="YYYY-MM-DD to (re)process a specific date. Empty = today (UTC)."),
    },
    tags=["ecommerce"],
)
def olist_platform():
    @task
    def resolve_date(**context) -> str: ## the "**" tells airflow to pass the internal metadata dict
        """Decide the ingestion date once, so all four tasks agree even across midnight."""
        requested = (context["params"].get("ingestion_date") or "").strip()
        day = date.fromisoformat(requested) if requested else datetime.now(timezone.utc).date()
        return day.isoformat()

    # Heavy imports (pyspark) 
    @task
    def ingest_olist(ingestion_date: str) -> str:
        from src.ingestion import ingest_olist as ingest
        return str(ingest.run(ingestion_date))

    @task(retries=3, retry_delay=timedelta(minutes=1))  
    def ingest_fx(ingestion_date: str) -> str:
        from src.ingestion import ingest_fx_rates as ingest
        return str(ingest.run(ingestion_date))

    @task
    def transform_olist(ingestion_date: str) -> str:
        from src.transform import transform_dataset
        return str(transform_dataset.run_all(ingestion_date))

    @task
    def transform_fx(ingestion_date: str) -> str:
        from src.transform import transform_fx_rates
        return str(transform_fx_rates.run(ingestion_date))

    day = resolve_date()
    ingest_o, transform_o = ingest_olist(day), transform_olist(day)
    ingest_f, transform_f = ingest_fx(day), transform_fx(day)
    ## orden de ejecucion de tareas
    ingest_o >> transform_o
    ingest_f >> transform_f


olist_platform()