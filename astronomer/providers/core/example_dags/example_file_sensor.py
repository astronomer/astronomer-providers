import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
    start = EmptyOperator(task_id="start")

    create_file = BashOperator(
        task_id="create_file",
        bash_command="sleep 10 && touch /usr/local/airflow/dags/example_file_async_sensor.txt",
    )

    # [START howto_sensor_filesystem_async]
    file_sensor_task = FileSensorAsync(
        task_id="file_sensor_task",
        filepath="/usr/local/airflow/dags/example_file_async_sensor.txt",
        fs_conn_id=FS_CONN_ID,
        poke_interval=3,
    )
    # [END howto_sensor_filesystem_async]

    delete_file = BashOperator(
        task_id="delete_file",
        bash_command="rm /usr/local/airflow/dags/example_file_async_sensor.txt",
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    start >> [file_sensor_task, create_file]
    [create_file, file_sensor_task] >> delete_file
    [file_sensor_task, create_file, delete_file] >> end
