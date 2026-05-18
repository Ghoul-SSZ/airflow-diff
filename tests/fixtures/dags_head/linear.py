from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="linear",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"],
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start")
    middle = BashOperator(task_id="middle", bash_command="echo middle")
    end = BashOperator(task_id="end", bash_command="echo end")
    start >> middle >> end
