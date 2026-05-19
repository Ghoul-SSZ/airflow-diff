"""Daily finance rollup that depends on orders_pipeline.

Showcase DAG #2. ~16 tasks across 3 TaskGroups. Waits on
orders_pipeline.publish_orders_ready via ExternalTaskSensor with an
execution_delta=timedelta(hours=1) bridge (orders runs hourly; this DAG runs
daily, so the daily run aligns with the last hourly upstream run).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

REPORT_BUCKET = Variable.get("report_bucket")


def _aggregate_revenue(**_):
    return True


def _aggregate_refunds(**_):
    return True


def _aggregate_margins(**_):
    return True


with DAG(
    dag_id="finance_rollup",
    description="Daily finance rollup",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["showcase", "finance"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("wait") as wait:
        wait_for_orders = ExternalTaskSensor(
            task_id="wait_for_orders",
            external_dag_id="orders_pipeline",
            external_task_id="publish_orders_ready",
            execution_delta=timedelta(hours=1),
            mode="reschedule",
            poke_interval=300,
            timeout=60 * 60 * 4,
        )

    with TaskGroup("aggregate") as aggregate:
        daily_revenue = PythonOperator(task_id="daily_revenue", python_callable=_aggregate_revenue)
        daily_refunds = PythonOperator(task_id="daily_refunds", python_callable=_aggregate_refunds)
        daily_margins = PythonOperator(task_id="daily_margins", python_callable=_aggregate_margins)
        category_breakdown = BashOperator(
            task_id="category_breakdown",
            bash_command="aggregate.sh category --date {{ ds }} --window {{ macros.ds_add(ds, -1) }}..{{ ds }}",
        )
        region_breakdown = BashOperator(
            task_id="region_breakdown",
            bash_command="aggregate.sh region --date {{ ds }} --window {{ macros.ds_add(ds, -1) }}..{{ ds }}",
        )
        cohort_metrics = BashOperator(
            task_id="cohort_metrics",
            bash_command="aggregate.sh cohorts --date {{ ds }} --window {{ macros.ds_add(ds, -30) }}..{{ ds }}",
        )

    with TaskGroup("report") as report:
        build_exec_dashboard = BashOperator(
            task_id="build_exec_dashboard",
            bash_command="report.sh exec_dashboard --out s3://" + REPORT_BUCKET + "/{{ ds_nodash }}/exec.html",
        )
        build_finance_pdf = BashOperator(
            task_id="build_finance_pdf",
            bash_command="report.sh finance_pdf --out s3://" + REPORT_BUCKET + "/{{ ds_nodash }}/finance.pdf",
        )
        build_ops_csv = BashOperator(
            task_id="build_ops_csv",
            bash_command="report.sh ops_csv --out s3://" + REPORT_BUCKET + "/{{ ds_nodash }}/ops.csv",
        )
        notify_finance = BashOperator(
            task_id="notify_finance",
            bash_command="notify.sh finance --report s3://" + REPORT_BUCKET + "/{{ ds_nodash }}/finance.pdf",
        )
        notify_ops = BashOperator(
            task_id="notify_ops",
            bash_command="notify.sh ops --report s3://" + REPORT_BUCKET + "/{{ ds_nodash }}/ops.csv",
        )

    archive_raw = BashOperator(
        task_id="archive_raw",
        bash_command="archive.sh raw --bucket " + REPORT_BUCKET + " --date {{ ds }}",
    )
    cleanup_tmp = BashOperator(
        task_id="cleanup_tmp",
        bash_command="cleanup.sh tmp --date {{ ds }}",
    )

    start >> wait >> aggregate >> report >> archive_raw >> cleanup_tmp >> end
