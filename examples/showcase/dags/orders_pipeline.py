"""Hourly orders ingestion + staging pipeline.

Showcase DAG #1. ~19 tasks across 4 TaskGroups, demonstrating real-world
patterns: Variable.get for bucket prefixes, BaseHook.get_connection for the
warehouse, Jinja-templated bash_command and SQL, and a publish marker that a
downstream daily DAG waits on via ExternalTaskSensor.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

WAREHOUSE = BaseHook.get_connection("warehouse")
BUCKET = Variable.get("warehouse_bucket")


def _check_schema(**_):  # noqa: D401
    """Stub validator — real impl would inspect column types."""
    return True


def _check_nulls(**_):
    return True


def _check_row_counts(**_):
    return True


with DAG(
    dag_id="orders_pipeline",
    description="Hourly orders ingestion + staging",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["showcase", "orders"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("extract") as extract:
        extract_orders = BashOperator(
            task_id="extract_orders",
            bash_command="extract.sh orders --date {{ ds }} --out s3://" + BUCKET + "/raw/orders/{{ ds_nodash }}/",
        )
        extract_customers = BashOperator(
            task_id="extract_customers",
            bash_command="extract.sh customers --date {{ ds }} --out s3://" + BUCKET + "/raw/customers/{{ ds_nodash }}/",
        )
        extract_inventory = BashOperator(
            task_id="extract_inventory",
            bash_command="extract.sh inventory --date {{ ds }} --out s3://" + BUCKET + "/raw/inventory/{{ ds_nodash }}/",
        )
        extract_returns = BashOperator(
            task_id="extract_returns",
            bash_command="extract.sh returns --date {{ ds }} --out s3://" + BUCKET + "/raw/returns/{{ ds_nodash }}/",
        )

    with TaskGroup("validate") as validate:
        schema_check = PythonOperator(task_id="schema_check", python_callable=_check_schema)
        null_check = PythonOperator(task_id="null_check", python_callable=_check_nulls)
        row_count_check = PythonOperator(task_id="row_count_check", python_callable=_check_row_counts)

    with TaskGroup("transform") as transform:
        enrich_orders = BashOperator(
            task_id="enrich_orders",
            bash_command="transform.sh enrich_orders --warehouse " + WAREHOUSE.host + " --date {{ ds }}",
        )
        denormalize_customers = BashOperator(
            task_id="denormalize_customers",
            bash_command="transform.sh denormalize_customers --warehouse " + WAREHOUSE.host + " --date {{ ds }}",
        )
        compute_line_items = BashOperator(
            task_id="compute_line_items",
            bash_command="transform.sh compute_line_items --warehouse " + WAREHOUSE.host + " --date {{ ds }}",
        )
        flag_anomalies = BashOperator(
            task_id="flag_anomalies",
            bash_command="transform.sh flag_anomalies --warehouse " + WAREHOUSE.host + " --date {{ ds }}",
        )
        dedupe = BashOperator(
            task_id="dedupe",
            bash_command="transform.sh dedupe --warehouse " + WAREHOUSE.host + " --date {{ ds }}",
        )

    with TaskGroup("load") as load:
        load_staging_orders = BashOperator(
            task_id="load_staging_orders",
            bash_command="load.sh orders --bucket " + BUCKET + " --date {{ ds }}",
        )
        load_staging_customers = BashOperator(
            task_id="load_staging_customers",
            bash_command="load.sh customers --bucket " + BUCKET + " --date {{ ds }}",
        )
        load_staging_inventory = BashOperator(
            task_id="load_staging_inventory",
            bash_command="load.sh inventory --bucket " + BUCKET + " --date {{ ds }}",
        )
        load_audit_log = BashOperator(
            task_id="load_audit_log",
            bash_command="load.sh audit --bucket " + BUCKET + " --date {{ ds }}",
        )

    publish_orders_ready = EmptyOperator(task_id="publish_orders_ready")

    start >> extract >> validate >> transform >> load >> publish_orders_ready >> end
