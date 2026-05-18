from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="with_delta",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    ExternalTaskSensor(
        task_id="wait",
        external_dag_id="some_upstream",
        external_task_id="finalize",
        execution_delta=timedelta(hours=1),
    )
