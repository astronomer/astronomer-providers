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

"""
Manually testing the async external task sensor requires a separate DAG for it to defer execution
until the second DAG is complete.
    1. Add this file and "external_task_wait_for_me.py" to your local airflow/dags/
    2. Once airflow is running, select "Trigger Dag w/ Config" for DAG: "test_external_task_async"
    3. Copy the timestamp and hit the Trigger button
    4. Select "Trigger DAG w/ Config" for DAG: "test_external_task_async_waits_for_me"
    5. Paste timestamp and hit trigger button
    6. Confirm that "test_external_task_async" defers until "test_external_task_async_waits_for_me"
       successfully completes, then resumes execution and finishes without issue.
"""

from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer_operators.core.sensors.external_task import ExternalTaskSensorAsync

default_args = {"start_date": days_ago(0)}
with DAG("test_external_task_async", schedule_interval="@daily", default_args=default_args) as dag:
    ext_task_sensor = ExternalTaskSensorAsync(
        task_id="external_task_async",
        external_task_id="wait_for_me",
        external_dag_id="test_external_task_async_waits_for_me",
    )
