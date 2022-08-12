import json
import unittest
from datetime import datetime
from unittest import mock

import pytest
from aiobotocore.session import ClientCreatorContext
from airflow.models.connection import Connection
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync
from astronomer.providers.amazon.aws.hooks.s3 import S3HookAsync


@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_without_get_connection():
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_get_connection(mock_connection):
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_aws_secrets(mock_get_connection):
    mock_conn = Connection(extra=json.dumps({"aws_access_key_id": "", "aws_secret_access_key": ""}))
    mock_get_connection.return_value = mock_conn

    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_hook_get_file_metadata(mock_client):
    """
    Test check_wildcard_key for a valid response
    :return:
    """
    test_resp_iter = [
        {
            "Contents": [
                {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
            ]
        }
    ]
    mock_paginator = mock.Mock()
    mock_paginate = mock.MagicMock()
    mock_paginate.__aiter__.return_value = test_resp_iter
    mock_paginator.paginate.return_value = mock_paginate

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
    task = await s3_hook_async.get_file_metadata(mock_client, "test_bucket", "test*")
    assert task == [
        {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
        {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
    ]


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async.head_object")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
async def test_s3_key_hook_get_head_object(mock_client, mock_head_object):
    """
    Test if the key exists and asserts True if key is found.
    :return:
    """
    mock_client.return_value.head_object.return_value = {"ContentLength": 0}
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    task = await s3_hook_async.get_head_object(mock_client.return_value, "s3://test_bucket/file")
    assert task == {"ContentLength": 0}


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_hook_get_head_object_with_error(mock_client):
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
    response = await s3_hook_async.get_head_object(mock_client, "s3://test_bucket/file", "test_bucket")
    assert response is None


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
@unittest.expectedFailure
async def test_s3_key_hook_get_head_object_raise_exception(mock_client):
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
        response = await s3_hook_async.get_head_object(mock_client, "s3://test_bucket/file", "test_bucket")
        assert isinstance(response, err)


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_hook_get_files_without_wildcard(mock_client):
    """
    Test get_files for a valid response
    :return:
    """
    test_resp_iter = [
        {
            "Contents": [
                {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
            ]
        }
    ]
    mock_paginator = mock.Mock()
    mock_paginate = mock.MagicMock()
    mock_paginate.__aiter__.return_value = test_resp_iter
    mock_paginator.paginate.return_value = mock_paginate

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
    response = await s3_hook_async.get_files(mock_client, "test_bucket", "test.txt", False)
    assert response == []


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_hook_get_files_with_wildcard(mock_client):
    """
    Test get_files for a valid response
    :return:
    """
    test_resp_iter = [
        {
            "Contents": [
                {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
            ]
        }
    ]
    mock_paginator = mock.Mock()
    mock_paginate = mock.MagicMock()
    mock_paginate.__aiter__.return_value = test_resp_iter
    mock_paginator.paginate.return_value = mock_paginate

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
    response = await s3_hook_async.get_files(mock_client, "test_bucket", "test.txt", True)
    assert response == []


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@pytest.mark.asyncio
async def test_s3_key_hook_list_keys(mock_client):
    """
    Test _list_keys for a valid response
    :return:
    """
    test_resp_iter = [
        {
            "Contents": [
                {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
                {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34)},
            ]
        }
    ]
    mock_paginator = mock.Mock()
    mock_paginate = mock.MagicMock()
    mock_paginate.__aiter__.return_value = test_resp_iter
    mock_paginator.paginate.return_value = mock_paginate

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    mock_client.get_paginator = mock.Mock(return_value=mock_paginator)
    response = await s3_hook_async._list_keys(mock_client, "test_bucket", "test*")
    assert response == ["test_key", "test_key2"]


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync._list_keys")
@pytest.mark.asyncio
async def test_s3_key_hook_is_keys_unchanged_false(mock_list_keys, mock_client):
    """
    Test is_key_unchanged gives False response
    :return:
    """

    mock_list_keys.return_value = ["test"]

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    response = await s3_hook_async.is_keys_unchanged(
        client=mock_client.return_value,
        bucket_name="test_bucket",
        prefix="test",
        inactivity_period=1,
        min_objects=1,
        previous_objects=set(),
        inactivity_seconds=0,
        allow_delete=True,
        last_activity_time=None,
    )

    assert response.get("status") == "pending"

    # test for the case when current_objects < previous_objects
    mock_list_keys.return_value = []

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    response = await s3_hook_async.is_keys_unchanged(
        client=mock_client.return_value,
        bucket_name="test_bucket",
        prefix="test",
        inactivity_period=1,
        min_objects=1,
        previous_objects=set("test"),
        inactivity_seconds=0,
        allow_delete=True,
        last_activity_time=None,
    )

    assert response.get("status") == "pending"


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync._list_keys")
@pytest.mark.asyncio
async def test_s3_key_hook_is_keys_unchanged_exception(mock_list_keys, mock_client):
    """
    Test is_key_unchanged gives AirflowException
    :return:
    """
    mock_list_keys.return_value = []

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")

    response = await s3_hook_async.is_keys_unchanged(
        client=mock_client.return_value,
        bucket_name="test_bucket",
        prefix="test",
        inactivity_period=1,
        min_objects=1,
        previous_objects=set("test"),
        inactivity_seconds=0,
        allow_delete=False,
        last_activity_time=None,
    )

    assert response == {"message": " test_bucket/test between pokes.", "status": "error"}


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync._list_keys")
@pytest.mark.asyncio
async def test_s3_key_hook_is_keys_unchanged_pending(mock_list_keys, mock_client):
    """
    Test is_key_unchanged gives AirflowException
    :return:
    """
    mock_list_keys.return_value = []

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")

    response = await s3_hook_async.is_keys_unchanged(
        client=mock_client.return_value,
        bucket_name="test_bucket",
        prefix="test",
        inactivity_period=1,
        min_objects=0,
        previous_objects=set(),
        inactivity_seconds=0,
        allow_delete=False,
        last_activity_time=None,
    )

    assert response.get("status") == "pending"


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync._list_keys")
@pytest.mark.asyncio
async def test_s3_key_hook_is_keys_unchanged_inactivity_error(mock_list_keys, mock_client):
    """
    Test is_key_unchanged gives AirflowException
    :return:
    """
    mock_list_keys.return_value = []

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")

    response = await s3_hook_async.is_keys_unchanged(
        client=mock_client.return_value,
        bucket_name="test_bucket",
        prefix="test",
        inactivity_period=0,
        min_objects=5,
        previous_objects=set(),
        inactivity_seconds=5,
        allow_delete=False,
        last_activity_time=None,
    )

    assert response == {
        "status": "error",
        "message": "FAILURE: Inactivity Period passed, not enough objects found in test_bucket/test",
    }


@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync._list_keys")
@pytest.mark.asyncio
async def test_s3_key_hook_is_keys_unchanged_true(mock_list_keys, mock_client):
    """
    Test is_key_unchanged gives AirflowException
    :return:
    """
    mock_list_keys.return_value = ["test_file"]

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    response = await s3_hook_async.is_keys_unchanged(
        client=mock_client.return_value,
        bucket_name="test_bucket",
        prefix="test",
        inactivity_period=1,
        min_objects=1,
        previous_objects=set("t"),
        inactivity_seconds=0,
        allow_delete=True,
        last_activity_time=datetime(2020, 8, 14, 17, 19, 34),
    )

    assert response == {
        "message": (
            "SUCCESS: Sensor found 1 objects at test_bucket/test. "
            "Waited at least 1 seconds, with no new objects uploaded."
        ),
        "status": "success",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_first_prefix, test_second_prefix",
    [
        ("async-prefix1/", "async-prefix2/"),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.s3.S3HookAsync.get_client_async")
async def test_s3_prefix_sensor_hook_list_prefixes(mock_client, test_first_prefix, test_second_prefix):
    """
    Test list_prefixes whether it returns a valid response
    """
    test_resp_iter = [{"CommonPrefixes": [{"Prefix": test_first_prefix}, {"Prefix": test_second_prefix}]}]
    mock_paginator = mock.Mock()
    mock_paginate = mock.MagicMock()
    mock_paginate.__aiter__.return_value = test_resp_iter
    mock_paginator.paginate.return_value = mock_paginate

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    mock_client.get_paginator = mock.Mock(return_value=mock_paginator)

    actual_output = await s3_hook_async.list_prefixes(mock_client, "test_bucket", "test")
    expected_output = [test_first_prefix, test_second_prefix]
    assert expected_output == actual_output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_prefix, mock_bucket",
    [
        ("async-prefix1", "test_bucket"),
        ("async-prefix2", "test_bucket"),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.s3.S3HookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.hooks.s3.S3HookAsync.list_prefixes")
async def test_s3_prefix_sensor_hook_check_for_prefix(
    mock_list_prefixes, mock_client, mock_prefix, mock_bucket
):
    """
    Test that _check_for_prefix method returns True when valid prefix is used and returns False
    when invalid prefix is used
    """
    mock_list_prefixes.return_value = ["async-prefix1/", "async-prefix2/"]

    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")

    response = await s3_hook_async._check_for_prefix(
        client=mock_client.return_value, prefix=mock_prefix, bucket_name=mock_bucket, delimiter="/"
    )

    assert response is True

    response = await s3_hook_async._check_for_prefix(
        client=mock_client.return_value,
        prefix="non-existing-prefix",
        bucket_name=mock_bucket,
        delimiter="/",
    )

    assert response is False


@pytest.mark.asyncio
@mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_s3_bucket_key")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_head_object")
@mock.patch("astronomer.providers.amazon.aws.hooks.s3.S3HookAsync.get_client_async")
async def test_s3__check_key_without_wild_card(mock_client, mock_head_object, mock_get_bucket_key):
    """Test _check_key function"""
    mock_get_bucket_key.return_value = "test_bucket", "test.txt"
    mock_head_object.return_value = {"ContentLength": 0}
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    response = await s3_hook_async._check_key(
        mock_client.return_value, "test_bucket", False, "s3://test_bucket/file/test.txt"
    )
    assert response is True


@pytest.mark.asyncio
@mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_s3_bucket_key")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_head_object")
@mock.patch("astronomer.providers.amazon.aws.hooks.s3.S3HookAsync.get_client_async")
async def test_s3__check_key_none_without_wild_card(mock_client, mock_head_object, mock_get_bucket_key):
    """Test _check_key function when get head object returns none"""
    mock_get_bucket_key.return_value = "test_bucket", "test.txt"
    mock_head_object.return_value = None
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    response = await s3_hook_async._check_key(
        mock_client.return_value, "test_bucket", False, "s3://test_bucket/file/test.txt"
    )
    assert response is False


@pytest.mark.asyncio
@mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_s3_bucket_key")
@mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_file_metadata")
@mock.patch("astronomer.providers.amazon.aws.hooks.s3.S3HookAsync.get_client_async")
async def test_s3__check_key_with_wild_card(mock_client, mock_get_file_metadata, mock_get_bucket_key):
    """Test _check_key function"""
    mock_get_bucket_key.return_value = "test_bucket", "test"
    mock_get_file_metadata.return_value = [
        {"Key": "test_key", "ETag": "etag1", "LastModified": datetime(2020, 8, 14, 17, 19, 34), "Size": 0},
        {"Key": "test_key2", "ETag": "etag2", "LastModified": datetime(2020, 8, 14, 17, 19, 34), "Size": 0},
    ]
    s3_hook_async = S3HookAsync(client_type="S3", resource_type="S3")
    response = await s3_hook_async._check_key(
        mock_client.return_value, "test_bucket", True, "test/example_s3_test_file.txt"
    )
    assert response is False
