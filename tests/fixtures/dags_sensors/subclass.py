from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor


class WrappedSensor(ExternalTaskSensor):
    """House wrapper to verify MRO-walk detection."""
    pass


with DAG(
    dag_id="subclass_sensor",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    WrappedSensor(
        task_id="wait",
        external_dag_id="some_upstream",
        external_task_id="finalize",
    )
