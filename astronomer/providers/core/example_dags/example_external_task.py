"""
Manually testing the async external task sensor requires a separate DAG for it to defer execution
until the second DAG is complete.
    1. Add this file and "example_external_task_wait_for_me.py" to your local airflow/dags/
    2. Once airflow is running, select "Trigger Dag w/ Config" for DAG: "test_external_task_async"
    3. Copy the timestamp and hit the Trigger button
    4. Select "Trigger DAG w/ Config" for DAG: "test_external_task_async_waits_for_me"
    5. Paste timestamp and hit trigger button
    6. Confirm that "test_external_task_async" defers until "test_external_task_async_waits_for_me"
       successfully completes, then resumes execution and finishes without issue.
"""

from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

with DAG("test_external_task_async", schedule_interval="@daily", start_date=datetime(2022, 1, 1)) as dag:
    ext_task_sensor = ExternalTaskSensorAsync(
        task_id="external_task_async",
        external_task_id="wait_for_me",
        external_dag_id="test_external_task_async_waits_for_me",
    )
