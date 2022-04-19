from unittest import mock

import pytest
from airflow import models
from impala.hiveserver2 import HiveServer2Connection, HiveServer2Cursor

from astronomer.providers.apache.hive.hooks.hive import HiveCliHookAsync

TEST_TABLE = "test_table"
TEST_SCHEMA = "test_schema"
TEST_POLLING_INTERVAL = 5
TEST_PARTITION = "state='FL'"
TEST_METASTORE_CONN_ID = "test_conn_id"


@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_hive_client")
def test_get_hive_client(mock_connect, mock_get_connection):
    """Checks the connection to hive client"""
    mock_connect.return_value = HiveServer2Connection
    mock_get_connection.return_value = models.Connection(
        conn_id="metastore_default",
        conn_type="metastore",
        port=10000,
        host="localhost",
        extra='{"auth": ""}',
        schema="default",
    )
    hook = HiveCliHookAsync(TEST_METASTORE_CONN_ID)
    result = hook.get_hive_client()
    assert isinstance(result, type(HiveServer2Connection))


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
async def test_partition_exists(mock_get_client, mock_get_connection, result, response):
    """
    Tests to check if a partition in given table in hive
    is found or not
    """
    hook = HiveCliHookAsync(metastore_conn_id=TEST_METASTORE_CONN_ID)
    hiveserver_connection = mock.AsyncMock(HiveServer2Connection)
    mock_get_client.return_value = hiveserver_connection
    cursor = mock.AsyncMock(HiveServer2Cursor)
    hiveserver_connection.cursor.return_value = cursor
    cursor.is_executing.return_value = False
    cursor.fetchall.return_value = result
    res = await hook.partition_exists(TEST_TABLE, TEST_SCHEMA, TEST_PARTITION, TEST_POLLING_INTERVAL)
    assert res == response
