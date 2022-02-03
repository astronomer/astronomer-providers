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


import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

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
@mock.patch("astronomer_operators.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_trigger_running(mock_object_exists):
    """
    Tests that the SnowflakeTrigger in
    """
    mock_object_exists.return_value = "success"
    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.cloud.hooks.gcs.GoogleBaseHook.get_connection")
@mock.patch("astronomer_operators.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_trigger_success(mock_object_exists, mock_get_conenction):
    """
    Tests that the SnowflakeTrigger in success case
    """
    mock_object_exists.return_value = "success"

    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "success", "message": "success"})
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.cloud.hooks.gcs.GoogleBaseHook.get_connection")
@mock.patch("astronomer_operators.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_trigger_exception(mock_conn, mock_get_connection):
    """
    Tests the SnowflkeTrigger does not fire if there is an exception.
    """
    mock_conn.return_value = "Test exception"

    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "error", "message": "Test exception"})


# @pytest.mark.asyncio
# @mock.patch("astronomer_operators.google.cloud.hooks.gcs.GCSAsyncHook")
# @mock.patch("astronomer_operators.google.cloud.triggers.gcs.Session")
# async def test_object_exists(mock_session, mock_trigger):
#     """
#     Asserts that a task is deferred and a GCSBlobTrigger will be fired
#     when the GCSObjectExistenceSensorAsync is executed.
#     """
# trigger = GCSBlobTrigger(
#     bucket=TEST_BUCKET,
#     object_name=TEST_OBJECT,
#     polling_period_seconds=TEST_POLLING_INTERVAL,
#     google_cloud_conn_id=TEST_GCP_CONN_ID,
# )
# task = asyncio.create_task(trigger.run().__anext__())
# await asyncio.sleep(0.5)

# # TriggerEvent was returned
# assert task.done() is True
# assert task.result() == TriggerEvent({"status": "success", "message": True})

# # Prevents error when task is destroyed while in "pending" state
# asyncio.get_event_loop().stop()


# trigger.run()
# mock_trigger.assert_called()
# mock_trigger.assert_called_once_with(
#     timeout=None, trigger=mock_trigger.return_value, method_name="execute_complete"
# )
