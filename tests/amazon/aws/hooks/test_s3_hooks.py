import json
from unittest import mock

import pytest
from aiobotocore.session import ClientCreatorContext
from airflow.models.connection import Connection

from astronomer_operators.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync


@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_without_get_connection():
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer_operators.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_get_connection(mock_connection):
    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)


@mock.patch("astronomer_operators.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync.get_connection")
@pytest.mark.asyncio
async def test_aws_base_hook_async_get_client_async_with_aws_secrets(mock_get_connection):
    mock_conn = Connection(extra=json.dumps({"aws_access_key_id": "", "aws_secret_access_key": ""}))
    mock_get_connection.return_value = mock_conn

    aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
    response = await aws_base_hook_async_obj.get_client_async()

    assert isinstance(response, ClientCreatorContext)
