import json
from unittest import mock

import pytest
from aiobotocore.session import ClientCreatorContext
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync


class TestAwsBaseHookAsync:
    @pytest.mark.asyncio
    async def test_get_client_async_without_get_connection(self):
        aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
        response = await aws_base_hook_async_obj.get_client_async()

        assert isinstance(response, ClientCreatorContext)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
    async def test_get_client_async_with_get_connection(self, mock_connection):
        aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
        response = await aws_base_hook_async_obj.get_client_async()

        assert isinstance(response, ClientCreatorContext)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
    async def test_get_client_async_with_aws_secrets(self, mock_get_connection):
        mock_conn = Connection(extra=json.dumps({"aws_access_key_id": "", "aws_secret_access_key": ""}))
        mock_get_connection.return_value = mock_conn

        aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3")
        response = await aws_base_hook_async_obj.get_client_async()

        assert isinstance(response, ClientCreatorContext)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
    async def test_get_client_async_with_aws_session(self, mock_get_connection):
        mock_conn = Connection(
            login="test", password="", extra=json.dumps({"region_name": "", "aws_session_token": ""})
        )
        mock_get_connection.return_value = mock_conn

        aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3", region_name=None)
        response = await aws_base_hook_async_obj.get_client_async()

        assert isinstance(response, ClientCreatorContext)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
    @mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_role_credentials")
    async def test_get_client_async_with_role_arn(self, mock_role_credentials, mock_get_connection):
        mock_conn = Connection(
            login="test",
            password="",
            extra=json.dumps({"region_name": "test", "role_arn": "arn:aws:iam::test:role/test"}),
        )
        mock_get_connection.return_value = mock_conn
        mock_role_credentials.return_value = {
            "AccessKeyId": "test",
            "SecretAccessKey": "test",
            "SessionToken": "test",
        }
        aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3", region_name=None)
        response = await aws_base_hook_async_obj.get_client_async()

        assert isinstance(response, ClientCreatorContext)

    @pytest.mark.asyncio
    @mock.patch("aiobotocore.session.get_session")
    @mock.patch("aiobotocore.session.ClientCreatorContext")
    @mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
    async def test_get_role_credentials(self, mock_get_connection, mock_client_creator, mock_session):
        mock_conn = Connection(
            login="test",
            password="",
            extra=json.dumps(
                {
                    "region_name": "test",
                    "role_arn": "arn:aws:iam::test:role/test",
                    "assume_role_method": "assume_role",
                }
            ),
        )
        mock_get_connection.return_value = mock_conn
        aws_base_hook_async_obj = AwsBaseHookAsync(client_type="S3", resource_type="S3", region_name=None)
        conn_config = AwsConnectionWrapper(
            conn=mock_get_connection,
            region_name=aws_base_hook_async_obj.region_name,
            botocore_config=aws_base_hook_async_obj.config,
            verify=aws_base_hook_async_obj.verify,
        )
        mock_session.create_client = mock_client_creator
        mock_client_creator.return_value.__aenter__.return_value.assume_role.return_value = {
            "Credentials": {"AccessKeyId": "test", "SecretAccessKey": "test", "SessionToken": "test"}
        }
        response = await aws_base_hook_async_obj.get_role_credentials(mock_session, conn_config)
        assert response == {"AccessKeyId": "test", "SecretAccessKey": "test", "SessionToken": "test"}
