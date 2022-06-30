import json
from asyncio import Future
from unittest import mock

import pytest
from airflow.models import Connection
from airflow.utils import db
from azure.core.exceptions import ResourceNotFoundError
from azure.identity.aio._credentials import DefaultAzureCredential
from azure.storage.blob._models import BlobProperties

from astronomer.providers.microsoft.azure.hooks.wasb import (
    BlobServiceClient,
    WasbHookAsync,
)

TEST_DATA_STORAGE_BLOB_NAME = "test_blob_providers_team.txt"
TEST_DATA_STORAGE_CONTAINER_NAME = "test-container-providers-team"
TEST_DATA_STORAGE_BLOB_PREFIX = TEST_DATA_STORAGE_BLOB_NAME[:10]

CONN_STRING = (
    "DefaultEndpointsProtocol=https;AccountName=testname;AccountKey=wK7BOz;EndpointSuffix=core.windows.net"
)
TEST_CONNECTION_TYPE = "wasb"
TEST_WASB_CONN_ID = "wasb_default"
TEST_CONNECTION_PROXIES = {"http": "http_proxy_uri", "https": "https_proxy_uri"}


class TestWasbHookAsyncConnection:
    def setup(self):
        db.merge_conn(Connection(conn_id="wasb_test_key", conn_type="wasb", login="login", password="key"))
        self.connection_type = "wasb"
        self.connection_string_id = "azure_test_connection_string"
        self.shared_key_conn_id = "azure_shared_key_test"
        self.ad_conn_id = "azure_AD_test"
        self.sas_conn_id = "sas_token_id"
        self.extra__wasb__sas_conn_id = "extra__sas_token_id"
        self.http_sas_conn_id = "http_sas_token_id"
        self.extra__wasb__http_sas_conn_id = "extra__http_sas_token_id"
        self.public_read_conn_id = "pub_read_id"
        self.managed_identity_conn_id = "managed_identity"

        self.proxies = {"http": "http_proxy_uri", "https": "https_proxy_uri"}

        db.merge_conn(
            Connection(
                conn_id=self.public_read_conn_id,
                conn_type=self.connection_type,
                host="https://accountname.blob.core.windows.net",
                extra=json.dumps({"proxies": self.proxies}),
            )
        )

        db.merge_conn(
            Connection(
                conn_id=self.connection_string_id,
                conn_type=self.connection_type,
                extra=json.dumps({"connection_string": CONN_STRING, "proxies": self.proxies}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.shared_key_conn_id,
                conn_type=self.connection_type,
                host="https://accountname.blob.core.windows.net",
                extra=json.dumps({"shared_access_key": "token", "proxies": self.proxies}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.ad_conn_id,
                conn_type=self.connection_type,
                host="conn_host",
                login="appID",
                password="appsecret",
                extra=json.dumps({"tenant_id": "token", "proxies": self.proxies}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.managed_identity_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps({"proxies": self.proxies}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.sas_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps({"sas_token": "token", "proxies": self.proxies}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.extra__wasb__sas_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps({"extra__wasb__sas_token": "token", "proxies": self.proxies}),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.http_sas_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps(
                    {"sas_token": "https://login.blob.core.windows.net/token", "proxies": self.proxies}
                ),
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.extra__wasb__http_sas_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps(
                    {
                        "extra__wasb__sas_token": "https://login.blob.core.windows.net/token",
                        "proxies": self.proxies,
                    }
                ),
            )
        )

    def test_key(self):
        """
        Tests that a db connection gets created successfully for a given login/password pair and
        that the hook is able to use this connection to generate a BlobServiceClient.
        """
        hook = WasbHookAsync(wasb_conn_id="wasb_test_key")
        assert hook.conn_id == "wasb_test_key"
        assert isinstance(hook.blob_service_client, BlobServiceClient)

    def test_public_read(self):
        """
        Tests that a db connection gets created successfully for anonymous public access read and
        that the hook is able to use this connection to generate a BlobServiceClient.
        """
        hook = WasbHookAsync(wasb_conn_id=self.public_read_conn_id, public_read=True)
        assert isinstance(hook.get_conn(), BlobServiceClient)

    def test_connection_string(self):
        """
        Tests that a db connection gets created successfully from a connection string and
        that the hook is able to use this connection to generate a BlobServiceClient.
        """
        hook = WasbHookAsync(wasb_conn_id=self.connection_string_id)
        assert hook.conn_id == self.connection_string_id
        assert isinstance(hook.get_conn(), BlobServiceClient)

    def test_shared_key_connection(self):
        """
        Tests that a db connection gets created successfully with the given shared access key and
        that the hook is able to use this connection to generate a BlobServiceClient.
        """
        hook = WasbHookAsync(wasb_conn_id=self.shared_key_conn_id)
        assert isinstance(hook.get_conn(), BlobServiceClient)

    def test_managed_identity(self):
        """
        Tests that a db connection gets created successfully with a managed identity and
        that the hook is able to use this connection to generate a BlobServiceClient.
        """
        hook = WasbHookAsync(wasb_conn_id=self.managed_identity_conn_id)
        assert isinstance(hook.get_conn(), BlobServiceClient)
        assert isinstance(hook.get_conn().credential, DefaultAzureCredential)

    @pytest.mark.parametrize(
        argnames="conn_id_str, extra_key",
        argvalues=[
            ("sas_conn_id", "sas_token"),
            ("extra__wasb__sas_conn_id", "extra__wasb__sas_token"),
            ("http_sas_conn_id", "sas_token"),
            ("extra__wasb__http_sas_conn_id", "extra__wasb__sas_token"),
        ],
    )
    def test_sas_token_connection(self, conn_id_str, extra_key):
        """
        Tests that a db connection gets created successfully with the given SAS tokens and
        that the hook is able to use this connection to generate a BlobServiceClient.
        """
        conn_id = self.__getattribute__(conn_id_str)
        hook = WasbHookAsync(wasb_conn_id=conn_id)
        conn = hook.get_conn()
        hook_conn = hook.get_connection(hook.conn_id)
        sas_token = hook_conn.extra_dejson[extra_key]
        assert isinstance(conn, BlobServiceClient)
        assert conn.url.endswith(sas_token + "/")

    @pytest.mark.parametrize(
        argnames="conn_id_str",
        argvalues=[
            "connection_string_id",
            "shared_key_conn_id",
            "ad_conn_id",
            "managed_identity_conn_id",
            "sas_conn_id",
            "extra__wasb__sas_conn_id",
            "http_sas_conn_id",
            "extra__wasb__http_sas_conn_id",
        ],
    )
    def test_connection_extra_arguments(self, conn_id_str):
        """
        Tests that the db connections are created with the correct extra arguments provided.
        Proxies are given as part of the extras while creating the connections and the same
        are tested that they get attached properly to the connections.
        """
        conn_id = self.__getattribute__(conn_id_str)
        hook = WasbHookAsync(wasb_conn_id=conn_id)
        conn = hook.get_conn()
        assert conn._config.proxy_policy.proxies == self.proxies

    def test_connection_extra_arguments_public_read(self):
        """
        Tests that the provided extra_arguments (in this case, proxies) are rightly
        attached to the public_read connection.
        """
        conn_id = self.public_read_conn_id
        hook = WasbHookAsync(wasb_conn_id=conn_id, public_read=True)
        conn = hook.get_conn()
        assert conn._config.proxy_policy.proxies == self.proxies


class BlobPropertiesAsyncIterator(BlobProperties):
    def __init__(self, end_range):
        self.end = end_range
        self.start = 0
        super().__init__(name=str(self.start))

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.start < self.end:
            self.start += 1
            return self
        else:
            raise StopAsyncIteration


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_blob_properties, expected_result",
    [
        (BlobProperties(), True),
        ("raise_resource_not_found", False),
    ],
)
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
async def test_check_for_blob(mock_service_client, mock_blob_properties, expected_result):
    """Tests check_for_blob function with mocked response."""
    hook = WasbHookAsync(wasb_conn_id=TEST_WASB_CONN_ID)
    mock_blob_client = mock_service_client.return_value.get_blob_client

    future = Future()
    if mock_blob_properties == "raise_resource_not_found":
        future.set_exception(ResourceNotFoundError())
    else:
        future.set_result(mock_blob_properties)
    mock_blob_client.return_value.get_blob_properties.return_value = future
    response = await hook.check_for_blob(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME, blob_name=TEST_DATA_STORAGE_CONTAINER_NAME
    )
    assert response == expected_result

    mock_blob_client.assert_called_once_with(
        container=TEST_DATA_STORAGE_CONTAINER_NAME, blob=TEST_DATA_STORAGE_CONTAINER_NAME
    )
    mock_blob_client.return_value.get_blob_properties.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blobs, expected_result",
    [
        (BlobPropertiesAsyncIterator(5), True),
        (BlobPropertiesAsyncIterator(0), False),
    ],
)
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
async def test_check_for_prefix(mock_service_client, blobs, expected_result):
    """Test check_for_prefix function with mocked response."""
    hook = WasbHookAsync(wasb_conn_id=TEST_WASB_CONN_ID)
    mock_container_client = mock_service_client.return_value.get_container_client

    mock_container_client.return_value.walk_blobs.return_value = blobs
    response = await hook.check_for_prefix(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME, prefix=TEST_DATA_STORAGE_BLOB_PREFIX
    )
    assert response == expected_result

    mock_container_client.assert_called_once_with(TEST_DATA_STORAGE_CONTAINER_NAME)
    mock_container_client.return_value.walk_blobs.assert_called()
