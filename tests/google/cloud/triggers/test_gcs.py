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
from gcloud.aio.storage import Bucket, Storage

from astronomer.providers.google.cloud.hooks.gcs import GCSHookAsync
from astronomer.providers.google.cloud.triggers.gcs import GCSBlobTrigger

TEST_BUCKET = "TEST_BUCKET"

TEST_OBJECT = "TEST_OBJECT"

TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"

TEST_POLLING_INTERVAL = 3.0

TEST_DAG_ID = "unit_tests_gcs_sensor"

TEST_HOOK_PARAMS = {}


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
        TEST_HOOK_PARAMS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger"
    assert kwargs == {
        "bucket": TEST_BUCKET,
        "object_name": TEST_OBJECT,
        "polling_period_seconds": TEST_POLLING_INTERVAL,
        "google_cloud_conn_id": TEST_GCP_CONN_ID,
        "hook_params": TEST_HOOK_PARAMS,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_trigger_success(mock_object_exists):
    """
    Tests that the GCSBlobTrigger is success case
    """
    mock_object_exists.return_value = "success"

    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "success", "message": "success"})
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_trigger_exception(mock_object_exists):
    """
    Tests the GCSBlobTrigger does fire if there is an exception.
    """
    mock_object_exists.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
    trigger = GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "error", "message": "Test exception"})

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exists,response",
    [
        (True, "success"),
        (False, "pending"),
    ],
)
async def test_object_exists(exists, response):
    """
    Tests to check if a particular object in Google Cloud Storage
    is found or not
    """
    hook = mock.AsyncMock(GCSHookAsync)
    storage = mock.AsyncMock(Storage)
    hook.get_storage_client.return_value = storage
    bucket = mock.AsyncMock(Bucket)
    storage.get_bucket.return_value = bucket
    bucket.blob_exists.return_value = exists
    trigger = GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    res = await trigger._object_exists(hook, TEST_BUCKET, TEST_OBJECT)
    assert res == response
    bucket.blob_exists.assert_called_once_with(blob_name=TEST_OBJECT)
