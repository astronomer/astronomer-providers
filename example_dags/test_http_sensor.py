from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer_operators.http import HttpSensorAsync

with DAG(
    "example_async_http_sensor", tags=["example", "async"], start_date=days_ago(2)
) as dag:
    # This task will continue to defer as it will receive 404 everytime
    async_http_sensor = HttpSensorAsync(
        task_id="async_http_sensor",
        http_conn_id="http_default",
        endpoint="non_existent_endpoint",
        request_params={},
        # TODO response_check is currently not supported
        # response_check=lambda response: "httpbin" in response.text,
        poke_interval=5,
    )

    # When passing 'response_check' it behaves same as 'HttpSensor'
    http_sensor_check = HttpSensorAsync(
        task_id="http_sensor_check",
        http_conn_id="http_default",
        endpoint="",
        request_params={},
        response_check=lambda response: "httpbin" in response.text,
        poke_interval=5,
    )
