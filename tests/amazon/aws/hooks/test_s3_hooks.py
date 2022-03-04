import asyncio
import json
import unittest
from unittest import mock

import pytest
from aiobotocore.session import ClientCreatorContext
from airflow.models.connection import Connection
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync
from astronomer.providers.amazon.aws.hooks.s3 import S3HookAsync


@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_without_get_connection():
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer.providers.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_get_connection(mock_connection):
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer.providers.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_aws_secrets(mock_get_connection):
    mock_conn = Connection(extra=json.dumps({"aws_access_key_id": "", "aws_secret_access_key": ""}))
    mock_get_connection.return_value = mock_conn

    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync._check_exact_key")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync._check_wildcard_key")
@pytest.mark.asyncio
async def test_s3_key_hook_check_key(mock_check_wildcard_key, mock_check_exact_key, mock_client):
    """
    Test for key with and without wildcard.
    :return:
    """
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")

    # test for wildcard=False
    asyncio.create_task(
        s3_hook_async.check_key(mock_client.return_value, "s3://test_bucket/file", "test_bucket", False)
    )
    await asyncio.sleep(0.5)
    mock_check_exact_key.assert_called_once_with(
        mock_client.return_value, "s3://test_bucket/file", "test_bucket"
    )

    # test for wildcard=True
    asyncio.create_task(
        s3_hook_async.check_key(mock_client.return_value, "s3://test_bucket/file", "test_bucket", True)
    )
    await asyncio.sleep(0.5)
    mock_check_wildcard_key.assert_called_once_with(
        mock_client.return_value, "s3://test_bucket/file", "test_bucket"
    )

    asyncio.get_event_loop().stop()


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_hook_check_exact_key(mock_client):
    """
    Test if the key exists and asserts True if key is found.
    :return:
    """
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    task = await s3_hook_async._check_exact_key(
        mock_client.return_value, "s3://test_bucket/file", "test_bucket"
    )

    assert task is True


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_hook_check_key_with_error(mock_client):
    """
    Test for 404 error if key not found and assert based on response.
    :return:
    """
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")

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
    response = await s3_hook_async._check_exact_key(mock_client, "s3://test_bucket/file", "test_bucket")
    assert response is False


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
@unittest.expectedFailure
async def test_s3_key_hook_check_key_raise_exception(mock_client):
    """
    Test for 500 error if key not found and assert based on response.
    :return:
    """
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")

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
        response = await s3_hook_async._check_exact_key(mock_client, "s3://test_bucket/file", "test_bucket")
        assert isinstance(response, err)
