from datetime import datetime

from airflow.models.dag import DAG

from astronomer.providers.core.sensors.filesystem import FileSensorAsync

with DAG(
    "example_async_file_sensor",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["async"],
) as dag:
    sensor_task = FileSensorAsync(  # type: ignore[no-untyped-call]
        task_id="my_file_sensor_task",
        filepath="/files/dags/example_async_file.py",
    )
