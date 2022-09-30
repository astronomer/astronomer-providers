import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.apache.hive.triggers.named_hive_partition import (
    NamedHivePartitionTrigger,
)

TEST_POLLING_INTERVAL = 5
TEST_PARTITION_NAMES = ["user.user_profile/city=delhi"]
TEST_METASTORE_CONN_ID = "test_conn_id"


class TestNamedHivePartitionTrigger:
    def test_hive_partition_trigger_serialization(self):
        """
        Asserts that the NamedHivePartitionTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = NamedHivePartitionTrigger(
            partition_names=TEST_PARTITION_NAMES,
            polling_interval=TEST_POLLING_INTERVAL,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "astronomer.providers.apache.hive.triggers.named_hive_partition.NamedHivePartitionTrigger"
        )
        assert kwargs == {
            "partition_names": TEST_PARTITION_NAMES,
            "polling_interval": TEST_POLLING_INTERVAL,
            "metastore_conn_id": TEST_METASTORE_CONN_ID,
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.check_partition_exists")
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
    async def test_hive_partition_trigger_success(self, mock_get_connection, mock_check_partition_exists):
        """Tests that the HivePartitionTrigger is success case when a partition exists in the given table"""
        mock_check_partition_exists.return_value = True

        trigger = NamedHivePartitionTrigger(
            partition_names=TEST_PARTITION_NAMES,
            polling_interval=TEST_POLLING_INTERVAL,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )

        generator = trigger.run()
        response = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": "Named hive partition found."}) == response

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.check_partition_exists")
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
    async def test_hive_partition_trigger_pending(self, mock_get_connection, mock_check_partition_exists):
        """Test that HivePartitionTrigger is in loop if partition isn't found."""
        mock_check_partition_exists.return_value = False

        trigger = NamedHivePartitionTrigger(
            partition_names=TEST_PARTITION_NAMES,
            polling_interval=TEST_POLLING_INTERVAL,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.check_partition_exists")
    @mock.patch("astronomer.providers.apache.hive.hooks.hive.HiveCliHookAsync.get_connection")
    async def test_hive_partition_trigger_exception(self, mock_get_connection, mock_check_partition_exists):
        """Tests the HivePartitionTrigger does fire if there is an exception."""
        mock_check_partition_exists.side_effect = Exception("Test exception")
        trigger = NamedHivePartitionTrigger(
            partition_names=TEST_PARTITION_NAMES,
            polling_interval=TEST_POLLING_INTERVAL,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        task = [i async for i in trigger.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
