from datetime import datetime
from airflow import DAG
from operators import GreetingOperator  # imported from plugins/ on sys.path

with DAG(dag_id="custom_op", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    GreetingOperator(task_id="hello", greeting="Hello", name="{{ var.value.user }}")
