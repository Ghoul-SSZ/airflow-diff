from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator

OUT = Dataset("s3://bucket/output")
IN = Dataset("s3://bucket/input")

with DAG(dag_id="dataset_dag", start_date=datetime(2024, 1, 1), schedule=[IN], catchup=False) as dag:
    BashOperator(task_id="produce", bash_command="echo out", outlets=[OUT])
