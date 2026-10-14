from datetime import datetime, timezone

import pytest

pytest.importorskip("airflow", reason="DAG tests run inside the Airflow container")

from airflow.configuration import conf

try:  # the DagBag moved in recent Airflow 3 releases; support both locations
    from airflow.dag_processing.dagbag import DagBag
except ImportError:
    from airflow.models.dagbag import DagBag

DAG_ID = "olist_platform"


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=conf.get("core", "dags_folder"))


@pytest.fixture(scope="module")
def dag(bag):
    return bag.dags[DAG_ID]


def test_all_dags_import_without_errors(bag):
    assert bag.import_errors == {}


def test_expected_tasks_exist(dag):
    assert set(dag.task_ids) == {
        "resolve_date", "ingest_olist", "transform_olist", "ingest_fx", "transform_fx"}


def test_each_transform_waits_for_its_own_ingestion(dag):
    ## checking the upstream dependencies of the transform tasks
    assert dag.get_task("transform_olist").upstream_task_ids == {"resolve_date", "ingest_olist"}
    assert dag.get_task("transform_fx").upstream_task_ids == {"resolve_date", "ingest_fx"}


def test_the_two_branches_are_independent(dag):
    assert dag.get_task("ingest_olist").upstream_task_ids == {"resolve_date"}
    assert dag.get_task("ingest_fx").upstream_task_ids == {"resolve_date"}


def test_only_the_api_ingestion_retries(dag):
    assert dag.get_task("ingest_fx").retries == 3
    for task_id in ("ingest_olist", "transform_olist", "transform_fx"):
        assert dag.get_task(task_id).retries == 0


def test_runs_cannot_overlap_and_nothing_is_backfilled(dag):
    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_resolve_date_uses_requested_date(dag):
    fn = dag.get_task("resolve_date").python_callable ## extract the pure python function from the task object
    assert fn(params={"ingestion_date": "2026-01-15"}) == "2026-01-15"


def test_resolve_date_defaults_to_today_utc(dag):
    fn = dag.get_task("resolve_date").python_callable
    assert fn(params={"ingestion_date": ""}) == datetime.now(timezone.utc).date().isoformat()