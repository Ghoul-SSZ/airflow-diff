from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

CONFIGS = [{"name": "alpha"}, {"name": "beta"}, {"name": "gamma"}]


def make_dag(cfg):
    with DAG(dag_id=f"factory_{cfg['name']}", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
        BashOperator(task_id="t", bash_command=f"echo {cfg['name']}")
    return dag


for cfg in CONFIGS:
    globals()[f"dag_{cfg['name']}"] = make_dag(cfg)
