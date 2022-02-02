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


from unittest import mock

import pytest

from astronomer_operators.google.cloud.triggers.gcs import GCSBlobTrigger

TEST_BUCKET = "TEST_BUCKET"

TEST_OBJECT = "TEST_OBJECT"

TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"

TEST_POLLING_INTERVAL = 3.0

TEST_DAG_ID = "unit_tests_gcs_sensor"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    # context = {}
    yield


def test_gcs_trigger_serialization():
    """
    Asserts that the GCSBlobTrigger correctly serializes its arguments
    and classpath.
    """
    print("running ")
    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.google.cloud.triggers.gcs.GCSBlobTrigger"
    assert kwargs == {
        "bucket": TEST_BUCKET,
        "object_name": TEST_OBJECT,
        "polling_period_seconds": TEST_POLLING_INTERVAL,
        "google_cloud_conn_id": TEST_GCP_CONN_ID,
    }


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.cloud.sensors.gcs.GCSBlobTrigger")
@mock.patch("astronomer_operators.google.cloud.sensors.gcs.GCSBlobTrigger._object_exists")
async def test_object_exists(mock_trigger, mock_object_exists):
    """
    Asserts that a task is deferred and a GCSBlobTrigger will be fired
    when the GCSObjectExistenceSensorAsync is executed.
    """
    mock_object_exists.return_value = True
    trigger = GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )
    trigger.run()
    mock_trigger.assert_called()
    mock_trigger.assert_called_once_with(
        timeout=None, trigger=mock_trigger.return_value, method_name="execute_complete"
    )
