from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="downstream",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream",
        external_task_id="finalize",
        # NOTE: missing execution_delta — schedules differ (@hourly vs @daily)
    )
