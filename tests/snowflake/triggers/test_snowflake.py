import asyncio
from datetime import timedelta
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSqlApiTrigger,
    SnowflakeTrigger,
)

TASK_ID = "snowflake_check"
POLL_INTERVAL = 1.0
LIFETIME = timedelta(minutes=59)
RENEWAL_DELTA = timedelta(minutes=54)
MODULE = "astronomer.providers.snowflake"


class TestSnowflakeTrigger:
    TRIGGER = SnowflakeTrigger(
        task_id=TASK_ID,
        poll_interval=POLL_INTERVAL,
        query_ids=["uuid", "uuid"],
        snowflake_conn_id="test_conn",
    )

    def test_snowflake_trigger_serialization(self):
        """
        Asserts that the SnowflakeTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeTrigger"
        assert kwargs == {
            "task_id": TASK_ID,
            "poll_interval": 1.0,
            "query_ids": ["uuid", "uuid"],
            "snowflake_conn_id": "test_conn",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "return_value,response",
        [
            (
                {"status": "error", "message": "test error"},
                TriggerEvent({"status": "error", "message": "test error"}),
            ),
            (
                {"status": "success", "query_ids": ["uuid", "uuid"]},
                TriggerEvent({"status": "success", "query_ids": ["uuid", "uuid"]}),
            ),
            (
                False,
                TriggerEvent(
                    {
                        "status": "error",
                        "message": f"{TASK_ID} " f"failed with terminal state: False",
                    }
                ),
            ),
        ],
    )
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
    async def test_snowflake_trigger_running(self, mock_get_query_status, return_value, response):
        """Tests that the SnowflakeTrigger in"""
        mock_get_query_status.return_value = return_value

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert response == actual

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
    async def test_snowflake_trigger_success(self, mock_get_first):
        """Tests that the SnowflakeTrigger in success case"""
        mock_get_first.return_value = {"status": "success", "query_ids": ["uuid", "uuid"]}

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is True
        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
    async def test_snowflake_trigger_failed(self, mock_get_first):
        """Tests the SnowflakeTrigger does not fire if it reaches a failed state."""
        mock_get_first.return_value = {
            "status": "error",
            "message": "The query is in the process of being aborted on the server side.",
            "type": "ABORTING",
        }

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(1)

        # TriggerEvent was returned
        assert task.done() is True
        assert task.result() == TriggerEvent(
            {
                "status": "error",
                "message": "The query is in the process of being aborted on the server side.",
                "type": "ABORTING",
            }
        )

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
    async def test_snowflake_trigger_exception(self, mock_query_status):
        """Tests the SnowflakeTrigger does not fire if there is an exception."""
        mock_query_status.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


class TestSnowflakeSqlApiTrigger:
    TRIGGER = SnowflakeSqlApiTrigger(
        poll_interval=POLL_INTERVAL,
        query_ids=["uuid"],
        snowflake_conn_id="test_conn",
        token_life_time=LIFETIME,
        token_renewal_delta=RENEWAL_DELTA,
    )

    def test_snowflake_sql_trigger_serialization(self):
        """
        Asserts that the SnowflakeSqlApiTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeSqlApiTrigger"
        assert kwargs == {
            "poll_interval": POLL_INTERVAL,
            "query_ids": ["uuid"],
            "snowflake_conn_id": "test_conn",
            "token_life_time": LIFETIME,
            "token_renewal_delta": RENEWAL_DELTA,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.snowflake_trigger.SnowflakeSqlApiTrigger.is_still_running")
    @mock.patch(f"{MODULE}.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status")
    async def test_snowflake_sql_trigger_running(self, mock_get_sql_api_query_status, mock_is_still_running):
        """Tests that the SnowflakeSqlApiTrigger in running by mocking is_still_running to true"""
        mock_is_still_running.return_value = True

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.snowflake_trigger.SnowflakeSqlApiTrigger.is_still_running")
    @mock.patch(f"{MODULE}.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status")
    async def test_snowflake_sql_trigger_completed(
        self, mock_get_sql_api_query_status, mock_is_still_running
    ):
        """
        Test SnowflakeSqlApiTrigger run method with success status and mock the get_sql_api_query_status result
        and  is_still_running to False.
        """
        mock_is_still_running.return_value = False
        statement_query_ids = ["uuid", "uuid1"]
        mock_get_sql_api_query_status.return_value = {
            "message": "Statement executed successfully.",
            "status": "success",
            "statement_handles": statement_query_ids,
        }

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "statement_query_ids": statement_query_ids}) == actual

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.snowflake_trigger.SnowflakeSqlApiTrigger.is_still_running")
    @mock.patch(f"{MODULE}.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status")
    async def test_snowflake_sql_trigger_failure_status(
        self, mock_get_sql_api_query_status, mock_is_still_running
    ):
        """Test SnowflakeSqlApiTrigger task is executed and triggered with failure status."""
        mock_is_still_running.return_value = False
        mock_response = {
            "status": "error",
            "message": "An error occurred when executing the statement. Check "
            "the error code and error message for details",
        }
        mock_get_sql_api_query_status.return_value = mock_response

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert TriggerEvent(mock_response) == actual

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.snowflake_trigger.SnowflakeSqlApiTrigger.is_still_running")
    @mock.patch(f"{MODULE}.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status")
    async def test_snowflake_sql_trigger_exception(
        self, mock_get_sql_api_query_status, mock_is_still_running
    ):
        """Tests the SnowflakeSqlApiTrigger does not fire if there is an exception."""
        mock_is_still_running.return_value = False
        mock_get_sql_api_query_status.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_response, expected_status",
        [
            ({"status": "success"}, False),
            ({"status": "error"}, False),
            ({"status": "running"}, True),
        ],
    )
    @mock.patch(f"{MODULE}.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.get_sql_api_query_status")
    async def test_snowflake_sql_trigger_is_still_running(
        self, mock_get_sql_api_query_status, mock_response, expected_status
    ):
        mock_get_sql_api_query_status.return_value = mock_response

        response = await self.TRIGGER.is_still_running(["uuid"])
        assert response == expected_status
