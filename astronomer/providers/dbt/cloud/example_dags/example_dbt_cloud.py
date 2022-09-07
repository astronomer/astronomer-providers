"""Example use of DBTCloudAsync related providers."""

import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperatorAsync

DBT_CLOUD_CONN_ID = os.getenv("ASTRO_DBT_CLOUD_CONN", "dbt_cloud_default")
DBT_CLOUD_ACCOUNT_ID = os.getenv("ASTRO_DBT_CLOUD_ACCOUNT_ID", 88348)
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "dbt_cloud_conn_id": DBT_CLOUD_CONN_ID,
    "account_id": DBT_CLOUD_ACCOUNT_ID,
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_dbt_cloud",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    tags=["example", "async", "dbt-cloud"],
    catchup=False,
) as dag:
    # [START howto_operator_dbt_cloud_run_job_async]
    trigger_dbt_job_run = DbtCloudRunJobOperatorAsync(
        task_id="trigger_dbt_job_run",
        job_id=125225,
        check_interval=10,
        timeout=300,
    )
    # [END howto_operator_dbt_cloud_run_job_async]
