import json
import os
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)

DATABRICKS_CONN_ID = os.environ.get("ASTRO_DATABRICKS_CONN_ID", "databricks_default")
DATABRICKS_CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "0806-193014-swab896")
DATABRICKS_JOB_ID = os.environ.get("DATABRICKS_JOB_ID", "1003")
# Notebook path as a Json object
# Example: {"notebook_path": "/Users/andrew.godwin@astronomer.io/quickstart_notebook"}
notebook_path = '{"notebook_path": "/Users/andrew.godwin@astronomer.io/quickstart_notebook"}'
NOTEBOOK_TASK = json.loads(os.environ.get("DATABRICKS_NOTEBOOK_TASK", notebook_path))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
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
        existing_cluster_id=DATABRICKS_CLUSTER_ID,
        notebook_task=NOTEBOOK_TASK,
        polling_period_seconds=30,
    )

    opr_run_now = DatabricksRunNowOperatorAsync(
        task_id="run_now",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        polling_period_seconds=30,
    )

    opr_submit_run >> opr_run_now
