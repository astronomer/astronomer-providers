import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.filesystem import FileSensorAsync

FS_CONN_ID = os.environ.get("ASTRO_FS_CONN_ID", "fs_default")

default_args = {
    "execution_timeout": timedelta(hours=6),
}

with DAG(
    dag_id="example_async_file_sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "core"],
) as dag:
    # [START howto_operator_file_sensor_async]
    file_sensor_task = FileSensorAsync(
        task_id="file_sensor_task",
        filepath="example_file_async_sensor.txt",
        fs_conn_id=FS_CONN_ID,
    )
    # [END howto_operator_file_sensor_async]
