import asyncio
from unittest import mock

import pytest

from astronomer.providers.amazon.aws.triggers.s3 import S3KeyTrigger


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
