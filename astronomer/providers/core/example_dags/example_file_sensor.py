import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.filesystem import FileSensorAsync

FS_CONN_ID = os.environ.get("ASTRO_FS_CONN_ID", "fs_default")

with DAG(
    dag_id="example_async_file_sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "async", "core"],
) as dag:
    # [START howto_operator_file_sensor_async]
    file_sensor_task = FileSensorAsync(
        task_id="file_sensor_task",
        filepath="example_file_async_sensor.txt",
        fs_conn_id=FS_CONN_ID,
        execution_timeout=timedelta(seconds=60),
    )
    # [END howto_operator_file_sensor_async]
