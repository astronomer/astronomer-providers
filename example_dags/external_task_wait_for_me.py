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
