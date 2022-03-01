from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.filesystem import FileSensorAsync

with DAG(
    "example_async_file_sensor",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["async"],
) as dag:
    sensor_task = FileSensorAsync(
        task_id="my_file_sensor_task",
        filepath="/files/dags/example_async_file.py",
    )
