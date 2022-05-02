import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.apache.hive.triggers.hive_partition import (
    HivePartitionTrigger,
)

TEST_TABLE = "test_table"
TEST_SCHEMA = "test_schema"
TEST_POLLING_INTERVAL = 5
TEST_PARTITION = "state='FL'"
TEST_METASTORE_CONN_ID = "test_conn_id"


def test_hive_partition_trigger_serialization():
    """
    Asserts that the HivePartitionTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.apache.hive.triggers.hive_partition.HivePartitionTrigger"
    assert kwargs == {
        "table": TEST_TABLE,
        "partition": TEST_PARTITION,
        "schema": TEST_SCHEMA,
        "polling_interval": TEST_POLLING_INTERVAL,
        "metastore_conn_id": TEST_METASTORE_CONN_ID,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.partition_exists")
@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
async def test_hive_partition_trigger_success(mock_get_connection, mock_partition_exists):
    """Tests that the HivePartitionTrigger is success case when a partition exists in the given table"""
    mock_partition_exists.return_value = "success"

    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "success", "message": "success"}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.partition_exists")
@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
async def test_hive_partition_trigger_pending(mock_get_connection, mock_partition_exists):
    """Test that HivePartitionTrigger is in loop if partition isn't found."""
    mock_partition_exists.return_value = "pending"

    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.partition_exists")
@mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
async def test_hive_partition_trigger_exception(mock_get_connection, mock_partition_exists):
    """Tests the HivePartitionTrigger does fire if there is an exception."""
    mock_partition_exists.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
    trigger = HivePartitionTrigger(
        table=TEST_TABLE,
        partition=TEST_PARTITION,
        schema=TEST_SCHEMA,
        polling_interval=TEST_POLLING_INTERVAL,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
