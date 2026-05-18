from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(dag_id="task_groups", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    start = BashOperator(task_id="start", bash_command="echo s")
    with TaskGroup(group_id="transform") as tg:
        clean = BashOperator(task_id="clean", bash_command="echo c")
        enrich = BashOperator(task_id="enrich", bash_command="echo e")
        clean >> enrich
    end = BashOperator(task_id="end", bash_command="echo end")
    start >> tg >> end
