import asyncio
from unittest import mock

import pytest

from astronomer.providers.amazon.aws.triggers.s3 import (
    S3KeySizeTrigger,
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
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
async def test_s3_key_trigger_run(mock_client):
    """
    Test if the task is run is in triggerr successfully.
    :return:
    """
    mock_client.return_value.check_key.return_value = True
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    with mock_client:
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()


def test_s3_key_size_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = S3KeySizeTrigger(
        bucket_key="s3://test_bucket/file", bucket_name="test_bucket", wildcard_match=True
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.s3.S3KeySizeTrigger"
    assert kwargs == {
        "bucket_name": "test_bucket",
        "bucket_key": "s3://test_bucket/file",
        "wildcard_match": True,
        "aws_conn_id": "aws_default",
        "hook_params": {},
        "check_fn_user": None,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
async def test_s3_key_size_trigger_run(mock_client):
    """
    Test if the task is run is in triggerr successfully.
    :return:
    """
    mock_client.return_value.check_key.return_value = True
    trigger = S3KeySizeTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    with mock_client:
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()


def test_s3_key_size_check_fn_trigger_run():
    """
    Test if the _check_fn returns True.
    :return:
    """
    trigger = S3KeySizeTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")

    response = trigger._check_fn(data=[])
    assert response is True


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
    """
    Test if the task is run is in triggerr successfully.
    :return:
    """
    mock_client.return_value.check_key.return_value = True
    trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
    with mock_client:
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()
