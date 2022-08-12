import asyncio
from datetime import datetime
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.s3 import (
    S3KeysUnchangedTrigger,
    S3KeyTrigger,
)


def test_s3_key_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket", wildcard_match=True)
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.s3.S3KeyTrigger"
    assert kwargs == {
        "bucket_name": "test_bucket",
        "bucket_key": "s3://test_bucket/file",
        "wildcard_match": True,
        "aws_conn_id": "aws_default",
        "hook_params": {},
        "check_fn": None,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
async def test_s3_key_trigger_run(mock_client):
    """
    Test if the task is run is in triggerr successfully.
    """
    mock_client.return_value.check_key.return_value = True
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    with mock_client:
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
async def test_s3_key_trigger_run_exception(mock_client):
    """Test if the task is run is in case of exception."""
    mock_client.side_effect = Exception("Unable to locate credentials")
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    generator = trigger.run()
    actual = await generator.asend(None)
    assert (
        TriggerEvent(
            {
                "message": "Unable to locate credentials",
                "status": "error",
            }
        )
        == actual
    )


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_files")
async def test_s3_key_trigger_run_check_fn_success(mock_get_files, mock_client):
    """Test if the task is run is in trigger with check_fn."""

    def dummy_check_fn(list_obj):
        return True

    mock_get_files.return_value = ["test"]
    mock_client.return_value.check_key.return_value = True
    trigger = S3KeyTrigger(
        bucket_key="s3://test_bucket/file", bucket_name="test_bucket", check_fn=dummy_check_fn
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "s3_objects": ["test"]}) == actual


def test_s3_keys_unchanged_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = S3KeysUnchangedTrigger(
        bucket_name="test_bucket",
        prefix="test",
        inactivity_period=1,
        min_objects=1,
        inactivity_seconds=0,
        previous_objects=None,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.s3.S3KeysUnchangedTrigger"
    assert kwargs == {
        "bucket_name": "test_bucket",
        "prefix": "test",
        "inactivity_period": 1,
        "min_objects": 1,
        "inactivity_seconds": 0,
        "previous_objects": set(),
        "allow_delete": 1,
        "aws_conn_id": "aws_default",
        "last_activity_time": None,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
async def test_s3_keys_unchanged_trigger_run(mock_client):
    """Test if the task is run is in trigger successfully."""
    mock_client.return_value.check_key.return_value = True
    trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
    with mock_client:
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()


def test_s3_keys_unchanged_trigger_raise_value_error():
    """
    Test if the S3KeysUnchangedTrigger raises Value error for negative inactivity_period.
    """
    with pytest.raises(ValueError):
        S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test", inactivity_period=-100)


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.is_keys_unchanged")
async def test_s3_keys_unchanged_trigger_run_success(mock_is_keys_unchanged, mock_client):
    """
    Test if the task is run is in triggerer successfully.
    """
    mock_is_keys_unchanged.return_value = {"status": "success"}
    trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success"}) == actual


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.is_keys_unchanged")
async def test_s3_keys_unchanged_trigger_run_pending(mock_is_keys_unchanged, mock_client):
    """Test if the task is run is in triggerer successfully."""
    mock_is_keys_unchanged.return_value = {"status": "pending", "last_activity_time": datetime.now()}
    trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()
