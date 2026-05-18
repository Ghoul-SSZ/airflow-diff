from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(dag_id="nested_params", start_date=datetime(2024, 1, 1), schedule=None, catchup=False,
         params={"region": "us-east-1", "bucket": "my-bucket"}) as dag:
    BashOperator(task_id="t", bash_command="echo {{ params.region }} {{ params.bucket }}")
