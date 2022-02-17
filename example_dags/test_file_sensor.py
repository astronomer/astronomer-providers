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

import airflow
from airflow.utils.dates import days_ago

from astronomer_operators.core.sensors.filesystem import FileSensorAsync

with airflow.DAG("example_async_file_sensor", start_date=days_ago(1), tags=["async"]) as dag:
    sensor_task = FileSensorAsync(
        task_id="my_file_sensor_task",
        filepath="/files/dags/example_async_file.py",
    )
