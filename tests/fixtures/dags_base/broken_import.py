from this_module_does_not_exist import nope  # noqa: F401

from datetime import datetime
from airflow import DAG
with DAG(dag_id="broken_import", start_date=datetime(2024, 1, 1)) as dag:
    pass
