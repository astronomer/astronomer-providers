import airflow
from airflow.utils.dates import days_ago

from astronomer.providers.core.sensors.filesystem import FileSensorAsync

with airflow.DAG("example_async_file_sensor", start_date=days_ago(1), tags=["async"]) as dag:
    sensor_task = FileSensorAsync(
        task_id="my_file_sensor_task",
        filepath="/files/dags/example_async_file.py",
    )
