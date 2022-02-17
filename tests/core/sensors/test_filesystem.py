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

import pytest
from airflow.exceptions import TaskDeferred
from airflow.utils.timezone import datetime

from astronomer_operators.core.sensors.filesystem import FileSensorAsync
from astronomer_operators.core.triggers.filesystem import FileTrigger

DEFAULT_DATE = datetime(2015, 1, 1)
TASK_ID = "example_file_sensor_task"
TEST_POLL_INTERVAL = 3.0
TEST_FILE_PATH = "/tmp/sample.py"


def test_filesystem_sensor_async(dag, context):
    """
    Asserts that a task is deferred and an FileTrigger will be fired
    when FileSensorAsync is provided with all required arguments
    """
    sensor = FileSensorAsync(
        filepath=TEST_FILE_PATH,
        task_id=TASK_ID,
        dag=dag,
    )

    with pytest.raises(TaskDeferred) as exc:
        sensor.execute(context)

    assert isinstance(exc.value.trigger, FileTrigger), "Trigger is not a FileTrigger"
