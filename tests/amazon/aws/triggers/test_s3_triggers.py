import asyncio
import unittest
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer_operators.amazon.aws.triggers.s3 import S3KeyTrigger


def test_s3_key_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket", wildcard_match=True)
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.amazon.aws.triggers.s3.S3KeyTrigger"
    assert kwargs == {
        "bucket_name": "test_bucket",
        "bucket_key": "s3://test_bucket/file",
        "wildcard_match": True,
        "aws_conn_id": "aws_default",
        "hook_params": {},
    }


@pytest.mark.asyncio
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3KeyTrigger._check_key")
async def test_s3_key_trigger_run(mock_check_key, mock_client):
    """
    Test if the task is run is in triggerr successfully.
    :return:
    """
    mock_check_key.return_value = True
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    with mock_client:
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()


@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3KeyTrigger._check_exact_key")
@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3KeyTrigger._check_wildcard_key")
@pytest.mark.asyncio
async def test_s3_key_trigger_check_key(mock_check_wildcard_key, mock_check_exact_key, mock_client):
    """
    Test for key with and without wildcard.
    :return:
    """
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")

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


@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_trigger_check_exact_key(mock_client):
    """
    Test if the key exists and asserts True if key is found.
    :return:
    """
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
    task = await trigger._check_exact_key(mock_client.return_value, "s3://test_bucket/file", "test_bucket")

    assert task is True


@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_trigger_check_key_with_error(mock_client):
    """
    Test for 404 error if key not found and assert based on response.
    :return:
    """
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")

    mock_client.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeServiceException",
                "Message": "Details/context around the exception or error",
            },
            "ResponseMetadata": {
                "RequestId": "1234567890ABCDEF",
                "HostId": "host ID data will appear here as a hash",
                "HTTPStatusCode": 404,
                "HTTPHeaders": {"header metadata key/values will appear here"},
                "RetryAttempts": 0,
            },
        },
        operation_name="s3",
    )
    response = await trigger._check_exact_key(mock_client, "s3://test_bucket/file", "test_bucket")
    assert response is False


@mock.patch("astronomer_operators.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
@unittest.expectedFailure
async def test_s3_key_trigger_check_key_raise_exception(mock_client):
    """
    Test for 500 error if key not found and assert based on response.
    :return:
    """
    trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")

    mock_client.head_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeServiceException",
                "Message": "Details/context around the exception or error",
            },
            "ResponseMetadata": {
                "RequestId": "1234567890ABCDEF",
                "HostId": "host ID data will appear here as a hash",
                "HTTPStatusCode": 500,
                "HTTPHeaders": {"header metadata key/values will appear here"},
                "RetryAttempts": 0,
            },
        },
        operation_name="s3",
    )
    with pytest.raises(ClientError) as err:
        response = await trigger._check_exact_key(mock_client, "s3://test_bucket/file", "test_bucket")
        assert isinstance(response, err)
