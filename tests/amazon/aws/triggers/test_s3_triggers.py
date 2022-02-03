import asyncio
import sys
from unittest import mock

import pytest

from astronomer_operators.amazon.aws.triggers.s3 import S3Trigger


def test_s3_key_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = S3Trigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket", wildcard_match=True)
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.amazon.aws.triggers.s3.S3Trigger"
    assert kwargs == {
        "bucket_name": "test_bucket",
        "bucket_key": "s3://test_bucket/file",
        "wildcard_match": True,
        "aws_conn_id": "aws_default",
        "verify": None,
    }


@pytest.mark.skipif(
    sys.version_info.minor <= 6 and sys.version_info.major <= 3,
    reason="No async on 3.6",
)
@pytest.mark.asyncio
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3Trigger._check_key")
async def test_s3_key_trigger_run(mock_check_key, mock_client):
    mock_check_key.return_value = True
    trigger = S3Trigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    with mock_client:
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()


@pytest.mark.skipif(
    sys.version_info.minor <= 6 and sys.version_info.major <= 3,
    reason="No async on 3.6",
)
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3Trigger._check_exact_key")
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3Trigger._check_wildcard_key")
@pytest.mark.asyncio
async def test_s3_key_trigger_check_key(mock_check_wildcard_key, mock_check_exact_key, mock_client):
    trigger = S3Trigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")

    # test for wildcard=False
    asyncio.create_task(
        trigger._check_key(mock_client.return_value, "s3://test_bucket/file", "test_bucket", False)
    )
    await asyncio.sleep(0.5)
    mock_check_exact_key.assert_called_once_with(
        mock_client.return_value, "s3://test_bucket/file", "test_bucket"
    )

    # test for wildcard=True
    asyncio.create_task(
        trigger._check_key(mock_client.return_value, "s3://test_bucket/file", "test_bucket", True)
    )
    await asyncio.sleep(0.5)
    mock_check_wildcard_key.assert_called_once_with(
        mock_client.return_value, "s3://test_bucket/file", "test_bucket"
    )

    asyncio.get_event_loop().stop()


@pytest.mark.skipif(
    sys.version_info.minor <= 6 and sys.version_info.major <= 3,
    reason="No async on 3.6",
)
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_trigger_check_exact_key(mock_client):
    trigger = S3Trigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    task = asyncio.create_task(
        trigger._check_exact_key(mock_client.return_value, "s3://test_bucket/file", "test_bucket")
    )
    await asyncio.sleep(0.5)

    assert task.result() is True

    asyncio.get_event_loop().stop()
