from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="missing_macro", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    BashOperator(task_id="t", bash_command="{{ macros.this_does_not_exist() }}")
