from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)

notebook_task = {"notebook_path": "/Users/andrew.godwin@astronomer.io/Quickstart Notebook"}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "databricks_dag",
    start_date=days_ago(0),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    opr_submit_run = DatabricksSubmitRunOperatorAsync(
        task_id="submit_run",
        databricks_conn_id="databricks_default",
        existing_cluster_id="0806-193014-swab896",
        notebook_task=notebook_task,
        polling_period_seconds=30,
    )

    opr_run_now = DatabricksRunNowOperatorAsync(
        task_id="run_now",
        databricks_conn_id="databricks_default",
        job_id=1003,
        polling_period_seconds=30,
    )

    opr_submit_run >> opr_run_now
