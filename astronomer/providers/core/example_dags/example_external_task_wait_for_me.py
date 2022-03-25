from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.time_sensor import TimeSensorAsync

default_args = {
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="test_external_task_async_waits_for_me",
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
    # [START howto_operator_time_sensor_async]
