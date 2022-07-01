import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.filesystem import FileSensorAsync

FS_CONN_ID = os.getenv("ASTRO_FS_CONN_ID", "fs_default")

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_async_file_sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "core"],
) as dag:
    # [START howto_sensor_filesystem_async]
    file_sensor_task = FileSensorAsync(
        task_id="file_sensor_task",
        filepath="example_file_async_sensor.txt",
        fs_conn_id=FS_CONN_ID,
    )
    # [END howto_sensor_filesystem_async]
