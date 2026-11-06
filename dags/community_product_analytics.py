"""Daily build of the community product analytics warehouse.

ingest -> staging -> marts -> sla_checks. The SLA task fails the run on any dataset
SLA breach; dagrun_timeout fails any run that has not finished 2 hours after start.
Written for Airflow 3.x (task-level `sla` was removed in Airflow 3).
"""
from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _ingest():
    import ingest
    if ingest.main() != 0:
        raise RuntimeError("ingest failed checksum verification")


def _transform(sql_file):
    import pipeline
    pipeline.transform(sql_file)


def _sla():
    import sla
    if sla.main() != 0:
        raise RuntimeError("dataset SLA breach, see ops.sla_results")


default_args = {
    "owner": "product-analytics-de",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="community_product_analytics",
    start_date=datetime(2026, 11, 1),
    schedule="0 6 * * *",
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["product-analytics", "sla"],
) as dag:
    ingest_task = PythonOperator(task_id="ingest", python_callable=_ingest)
    staging = PythonOperator(task_id="staging", python_callable=_transform,
                             op_args=["01_staging.sql"])
    marts = PythonOperator(task_id="marts", python_callable=_transform, op_args=["02_marts.sql"])
    sla_checks = PythonOperator(task_id="sla_checks", python_callable=_sla, retries=0)

    ingest_task >> staging >> marts >> sla_checks
