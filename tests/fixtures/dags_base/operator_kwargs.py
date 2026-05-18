"""Fixture for testing wide literal-kwarg capture.

Each task exercises a different category of non-template kwarg to confirm
the renderer captures behavior-affecting attributes beyond the original
5-field hardcoded list.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


def _noop_callback(context):
    """A callable on_failure_callback — should be SKIPPED by the renderer."""
    pass


with DAG(
    dag_id="operator_kwargs",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:
    # Task A: every captured kwarg is set to a NON-default value.
    BashOperator(
        task_id="explicit_kwargs",
        bash_command="echo a",
        retries=5,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
        pool="my_pool",
        pool_slots=2,
        queue="my_queue",
        priority_weight=10,
        trigger_rule="all_done",
        depends_on_past=True,
        wait_for_downstream=True,
        email=["alerts@example.com"],
        email_on_failure=False,
        email_on_retry=False,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=15),
        executor_config={"KubernetesExecutor": {"image": "custom:1.0"}},
        on_failure_callback=_noop_callback,  # callable → must NOT be captured
        owner="data-team",  # blocklisted → must NOT be captured
        doc_md="Some docs",  # blocklisted → must NOT be captured
    )

    # Task B: everything left at defaults — only template fields should appear
    # in the rendered output; literal capture should add nothing (or near-nothing,
    # since most defaults will compare equal).
    BashOperator(task_id="defaults_only", bash_command="echo b")
