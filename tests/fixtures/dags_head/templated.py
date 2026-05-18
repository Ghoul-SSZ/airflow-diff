from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(dag_id="templated", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    BashOperator(
        task_id="copy_bucket",
        bash_command="aws s3 cp s3://{{ var.value.bucket }}/{{ ds }} /tmp/in",
    )
    BashOperator(
        task_id="copy_conn",
        bash_command="psql -h {{ conn.warehouse.host }} -c 'select 1'",
    )
    # Demonstrates Variable.get() inside Python at DAG-build time:
    region = Variable.get("region", default_var=None)
    BashOperator(task_id="show_region", bash_command=f"echo {region}")
