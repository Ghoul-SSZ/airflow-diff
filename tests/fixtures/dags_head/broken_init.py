from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="broken_init", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    # trigger_rule='invalid_rule' raises AirflowException during operator __init__
    BashOperator(task_id="t", bash_command="echo hi", trigger_rule="invalid_rule")
