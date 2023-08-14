import json
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, Mock

import boto3
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
    @mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync._refresh_credentials")
    async def test_get_client_async_with_role_arn(self, mock_role_credentials, mock_get_connection):
        mock_conn = Connection(
            login="test",
            password="",
            extra=json.dumps({"region_name": "test", "role_arn": "arn:aws:iam::test:role/test"}),
        )
        mock_get_connection.return_value = mock_conn
        mock_role_credentials.return_value = {
            "access_key": "test",
            "secret_key": "test",
            "token": "test",
            "expiry_time": "2022-10-10T00:00:00+00:00",
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


ASSUME_ROLE_RESPONSE = {
    "Credentials": {
        "AccessKeyId": "mock_access_key",
        "SecretAccessKey": "mock_secret_key",
        "SessionToken": "mock_session_token",
        "Expiration": "2023-08-16T00:00:00Z",
    },
    "ResponseMetadata": {"HTTPStatusCode": 200},
}


def mock_boto3_client(service_name, **kwargs):
    if service_name == "sts":
        client = MagicMock()
        client.assume_role.return_value = ASSUME_ROLE_RESPONSE
        return client
    return Mock()


class MockAwsConnectionWrapper:
    def __init__(self, **kwargs):
        self.role_arn = "mock_role_arn"
        self.assume_role_kwargs = {"RoleArn": self.role_arn}

    @property
    def session_kwargs(self):
        return {"region_name": "mock_region"}


def mock_get_connection(self, conn_id):
    return MockAwsConnectionWrapper()


@pytest.fixture
def mock_aws_base_hook_async(monkeypatch):
    monkeypatch.setattr(boto3, "client", mock_boto3_client)
    monkeypatch.setattr(AwsBaseHookAsync, "get_connection", mock_get_connection)
    return AwsBaseHookAsync()


def test_create_basic_session(mock_aws_base_hook_async):
    session_kwargs = {"region_name": "us-east-1"}
    session = mock_aws_base_hook_async._create_basic_session(session_kwargs)
    assert session.region_name == "us-east-1"


def test_assume_role(mock_aws_base_hook_async):
    sts_client = boto3.client("sts")
    conn_config = MockAwsConnectionWrapper(role_arn="mock_role_arn")
    result = mock_aws_base_hook_async._assume_role(sts_client, conn_config)
    assert result == ASSUME_ROLE_RESPONSE


@mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync._assume_role")
@mock.patch("astronomer.providers.amazon.aws.hooks.base_aws.AwsBaseHookAsync.get_connection")
def test_refresh_credentials(mock_get_connection, mock_assume_role, mock_aws_base_hook_async):
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

    mock_response = {"ResponseMetadata": {"HTTPStatusCode": 200}, "Credentials": {"AccessKeyId": "key", "SecretAccessKey": "secret", "SessionToken": "token", "Expiration": datetime(2015, 1, 1)}}
    mock_assume_role.return_value = mock_response

    credentials = mock_aws_base_hook_async._refresh_credentials()
    assert credentials["access_key"] == "key"
    assert credentials["secret_key"] == "secret"
    assert credentials["token"] == "token"
    assert credentials["expiry_time"] == datetime(2015, 1, 1).isoformat()
