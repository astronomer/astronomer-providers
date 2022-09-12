"""Example use of DBTCloudAsync related providers."""

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.timezone import datetime

from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperatorAsync
from astronomer.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensorAsync

DBT_CLOUD_CONN_ID = os.getenv("ASTRO_DBT_CLOUD_CONN", "dbt_cloud_default")
DBT_CLOUD_ACCOUNT_ID = os.getenv("ASTRO_DBT_CLOUD_ACCOUNT_ID", 12345)
DBT_CLOUD_JOB_ID = int(os.getenv("ASTRO_DBT_CLOUD_JOB_ID", 12345))
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
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    # [START howto_operator_dbt_cloud_run_job_async]
    trigger_dbt_job_run_async = DbtCloudRunJobOperatorAsync(
        task_id="trigger_dbt_job_run_async",
        job_id=DBT_CLOUD_JOB_ID,
        check_interval=10,
        timeout=300,
    )
    # [END howto_operator_dbt_cloud_run_job_async]

    trigger_job_run2 = DbtCloudRunJobOperator(
        task_id="trigger_job_run2",
        job_id=DBT_CLOUD_JOB_ID,
        wait_for_termination=False,
        additional_run_config={"threads_override": 8},
    )

    # [START howto_operator_dbt_cloud_run_job_sensor_async]
    job_run_sensor_async = DbtCloudJobRunSensorAsync(
        task_id="job_run_sensor_async", run_id=trigger_job_run2.output, timeout=20
    )
    # [END howto_operator_dbt_cloud_run_job_sensor_async]

    start >> trigger_dbt_job_run_async >> end
    start >> trigger_job_run2 >> job_run_sensor_async >> end
