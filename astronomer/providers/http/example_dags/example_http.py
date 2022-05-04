import os
from datetime import timedelta

from airflow import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.http.sensors.http import HttpSensorAsync

HTTP_CONN_ID = os.getenv("ASTRO_HTTP_CONN_ID", "http_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}


with DAG(
    dag_id="example_async_http_sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "http"],
) as dag:
    # [START howto_operator_http_sensor_async]
    async_http_sensor = HttpSensorAsync(
        task_id="async_http_sensor",
        http_conn_id=HTTP_CONN_ID,
        endpoint="",
        request_params={},
        # TODO response_check is currently not supported
        # response_check=lambda response: "httpbin" in response.text,
        poke_interval=5,
    )
    # [END howto_operator_file_sensor_async]

    async_http_sensor
