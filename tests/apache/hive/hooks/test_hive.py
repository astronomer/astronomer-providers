from unittest import mock

from airflow import models
from impala.hiveserver2 import HiveServer2Connection

from astronomer.providers.apache.hive.hooks.hive import HiveCliHookAsync

TEST_METASTORE_CONN_ID = "test_conn_id"


@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
def test_get_hive_client(mock_get_connection):
    """Checks the connection to hive client"""
    mock_get_connection.return_value = models.Connection(
        conn_id="metastore_default",
        conn_type="metastore",
        port=10000,
        host="localhost",
        extra='{"use_beeline": true, "auth": ""}',
        schema="default",
    )
    hook = HiveCliHookAsync(TEST_METASTORE_CONN_ID)
    result = hook.get_hive_client()
    assert isinstance(result, HiveServer2Connection)
