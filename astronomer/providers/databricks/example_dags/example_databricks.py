import json
import os
from datetime import timedelta
from typing import Dict, Optional

from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)

DATABRICKS_CONN_ID = os.environ.get("ASTRO_DATABRICKS_CONN_ID", "databricks_default")
# Notebook path as a Json object
# Example: {"notebook_path": "/Users/pankaj.singh@astronomer.io/quick_start"}
notebook_task = '{"notebook_path": "/Users/pankaj.singh@astronomer.io/quick_start"}'
NOTEBOOK_TASK = json.loads(os.environ.get("DATABRICKS_NOTEBOOK_TASK", notebook_task))
notebook_params: Optional[Dict[str, str]] = {"Variable": "5"}
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}

new_cluster = {
    "spark_version": "7.3.x-scala2.12",
    "num_workers": 2,
    "node_type_id": "i3en.large",
}


with DAG(
    dag_id="example_async_databricks",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "databricks"],
) as dag:
    opr_submit_run = DatabricksSubmitRunOperatorAsync(
        task_id="submit_run",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=new_cluster,
        notebook_task=NOTEBOOK_TASK,
        do_xcom_push=True,
    )

    opr_run_now = DatabricksRunNowOperatorAsync(
        task_id="run_now",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id="{{ task_instance.xcom_pull(task_ids='submit_run', dag_id='example_async_databricks', key='job_id') }}",
        notebook_params=notebook_params,
    )

opr_submit_run >> opr_run_now
