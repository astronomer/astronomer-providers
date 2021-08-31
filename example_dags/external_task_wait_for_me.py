import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.utils.dates import days_ago
from airflow.utils.timezone import utcnow

default_args = {"start_date": days_ago(0)}
with DAG(
    "test_external_task_async_waits_for_me",
    schedule_interval="@daily",
    default_args=default_args,
) as dag:
    wait_for_me = TimeSensorAsync(
        task_id="wait_for_me",
        target_time=utcnow() + datetime.timedelta(seconds=3),
    )
    complete = DummyOperator(task_id="complete")
    wait_for_me >> complete
