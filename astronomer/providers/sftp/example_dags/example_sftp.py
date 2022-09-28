import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.sftp.sensors.sftp import SFTPSensorAsync

SFTP_CONN_ID = os.getenv("ASTRO_SFTP_CONN_ID", "sftp_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


with DAG(
    dag_id="example_async_sftp_sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "sftp"],
) as dag:
    # [START howto_sensor_sftp_async]
    async_sftp_sensor = SFTPSensorAsync(
        task_id="async_sftp_sensor",
        sftp_conn_id=SFTP_CONN_ID,
        path="path/on/sftp/server",
        file_pattern="*.csv",
        poke_interval=5,
    )
    # [END howto_sensor_sftp_async]

    async_sftp_sensor
