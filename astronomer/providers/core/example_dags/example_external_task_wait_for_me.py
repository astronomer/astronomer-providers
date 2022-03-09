from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.utils.timezone import datetime, utcnow

with DAG(
    "test_external_task_async_waits_for_me",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
) as dag:
    wait_for_me = TimeSensorAsync(
        task_id="wait_for_me",
        target_time=utcnow() + timedelta(seconds=3),
    )
    complete = DummyOperator(task_id="complete")
    wait_for_me >> complete
