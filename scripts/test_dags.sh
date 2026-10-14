#!/usr/bin/env bash
# Run the DAG integrity tests inside the Airflow image, in an isolated Compose project.
set -euo pipefail
cd "$(dirname "$0")/.."

export AIRFLOW_UID="$(id -u)"
[ -f .env ] || cp .env.example .env      # CI has no .env; the example values are enough for tests
mkdir -p dags logs                        # create on the host, so Docker doesn't create them as root

COMPOSE="docker compose -p olist_test"    # separate project: never touches the dev stack or its volumes

cleanup() { $COMPOSE down -v --remove-orphans; }
trap cleanup EXIT

$COMPOSE build airflow-scheduler # build airflow container 
$COMPOSE run --rm --no-deps -w /opt/airflow/pipeline_code airflow-scheduler \
    python -m pytest tests/test_dags.py -v -p no:cacheprovider