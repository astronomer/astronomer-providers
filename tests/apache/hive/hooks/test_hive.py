from unittest import mock
from unittest.mock import PropertyMock

import pytest
from airflow import models
from impala.hiveserver2 import HiveServer2Connection, HiveServer2Cursor

from astronomer.providers.apache.hive.hooks.hive import HiveCliHookAsync

TEST_TABLE = "test_table"
TEST_SCHEMA = "test_schema"
TEST_POLLING_INTERVAL = 5
TEST_PARTITION = "state='FL'"
TEST_METASTORE_CONN_ID = "metastore_default"
TEST_CONN_TYPE = "metastore"
TEST_PORT = 10000
TEST_HOST = "localhost"


class TestHiveCliHookAsync:
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
    @mock.patch("airflow.configuration.AirflowConfigParser.get")
    @mock.patch("impala.hiveserver2.connect")
    def test_get_hive_client_with_conf(self, mock_get_connect, mock_get_conf, mock_get_connection):
        """Checks the connection to hive client"""
        mock_get_connect.return_value = mock.AsyncMock(HiveServer2Connection)
        mock_get_conf.return_value = "kerberos"
        mock_get_connection.return_value = models.Connection(
            conn_id=TEST_METASTORE_CONN_ID,
            conn_type=TEST_CONN_TYPE,
            port=TEST_PORT,
            host=TEST_HOST,
        )
        hook = HiveCliHookAsync(TEST_METASTORE_CONN_ID)
        result = hook.get_hive_client()
        assert isinstance(result, HiveServer2Connection)

    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
    @mock.patch("impala.hiveserver2.connect")
    def test_get_hive_client(self, mock_get_connect, mock_get_connection):
        """Checks the connection to hive client"""
        mock_get_connect.return_value = mock.AsyncMock(HiveServer2Connection)
        mock_get_connection.return_value = models.Connection(
            conn_id=TEST_METASTORE_CONN_ID,
            conn_type=TEST_CONN_TYPE,
            port=TEST_PORT,
            host=TEST_HOST,
        )
        hook = HiveCliHookAsync(TEST_METASTORE_CONN_ID)
        result = hook.get_hive_client()
        assert isinstance(result, HiveServer2Connection)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "result,response",
        [
            (["123"], "success"),
            ([], "failure"),
        ],
    )
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_hive_client")
    async def test_partition_exists(self, mock_get_client, mock_get_connection, result, response):
        """
        Tests to check if a partition in given table in hive
        is found or not
        """
        hook = HiveCliHookAsync(metastore_conn_id=TEST_METASTORE_CONN_ID)
        hiveserver_connection = mock.AsyncMock(HiveServer2Connection)
        mock_get_client.return_value = hiveserver_connection
        cursor = mock.AsyncMock(HiveServer2Cursor)
        hiveserver_connection.cursor.return_value = cursor
        cursor.is_executing = PropertyMock(side_effect=[True, False])
        cursor.fetchall.return_value = result
        res = await hook.partition_exists("test_table", TEST_SCHEMA, TEST_PARTITION, TEST_POLLING_INTERVAL)
        assert res == response

    @pytest.mark.parametrize(
        "partition,expected",
        [
            ("user_profile/city=delhi", ("default", "user_profile", "city=delhi")),
            ("user.user_profile/city=delhi", ("user", "user_profile", "city=delhi")),
        ],
    )
    def test_parse_partition_name_success(self, partition, expected):
        """Assert that `parse_partition_name` correctly parse partition string"""
        actual = HiveCliHookAsync.parse_partition_name(partition)
        assert actual == expected

    def test_parse_partition_name_exception(self):
        """Assert that `parse_partition_name` throw exception if partition string not correct"""
        with pytest.raises(ValueError):
            HiveCliHookAsync.parse_partition_name("user_profile.city=delhi")

    @pytest.mark.parametrize(
        "result,expected",
        [
            (["123"], True),
            ([], False),
        ],
    )
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_hive_client")
    def test_check_partition_exists(self, mock_get_client, mock_get_connection, result, expected):
        """Assert that `check_partition_exists` return True if partition found else return False."""
        hook = HiveCliHookAsync(metastore_conn_id=TEST_METASTORE_CONN_ID)
        hiveserver_connection = mock.AsyncMock(HiveServer2Connection)
        mock_get_client.return_value = hiveserver_connection
        cursor = mock.AsyncMock(HiveServer2Cursor)
        hiveserver_connection.cursor.return_value = cursor
        cursor.is_executing.return_value = False
        cursor.fetchall.return_value = result
        actual = hook.check_partition_exists(TEST_SCHEMA, "test_table", TEST_PARTITION)
        assert actual == expected
