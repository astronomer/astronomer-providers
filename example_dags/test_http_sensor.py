# Copyright 2022 Astronomer Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer_operators.http.operators.http import HttpSensorAsync

with DAG("example_async_http_sensor", tags=["example", "async"], start_date=days_ago(2)) as dag:
    # This task will continue to defer as it will receive 404 every time
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
