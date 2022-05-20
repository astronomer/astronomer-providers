import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.time_sensor import TimeSensorAsync

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}

with DAG(
    dag_id="example_external_task_async_waits_for_me",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "core"],
) as dag:
    # [START howto_operator_time_sensor_async]
    wait_for_me = TimeSensorAsync(
        task_id="wait_for_me",
        target_time=(datetime.utcnow() + timedelta(seconds=3)).time(),
    )
    # [END howto_operator_time_sensor_async]
