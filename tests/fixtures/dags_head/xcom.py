from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="xcom", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    BashOperator(task_id="upstream", bash_command="echo data")
    BashOperator(
        task_id="downstream",
        bash_command="echo {{ ti.xcom_pull(task_ids='upstream') }}",
    )
