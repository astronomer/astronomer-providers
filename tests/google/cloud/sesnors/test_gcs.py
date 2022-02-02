#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from unittest import TestCase

import pytest
from airflow.exceptions import TaskDeferred

from astronomer_operators.google.cloud.sensors.gcs import GCSObjectExistenceSensorAsync
from astronomer_operators.google.cloud.triggers.gcs import GCSBlobTrigger

TEST_BUCKET = "TEST_BUCKET"

TEST_OBJECT = "TEST_OBJECT"

TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"

TEST_DAG_ID = "unit_tests_gcs_sensor"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    # context = {}
    yield


class TestGoogleCloudStorageObjectSensor(TestCase):
    def test_should_pass_argument_to_hook(self):
        task = GCSObjectExistenceSensorAsync(
            task_id="task-id",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, GCSBlobTrigger), "Trigger is not a GCSBlobTrigger"
