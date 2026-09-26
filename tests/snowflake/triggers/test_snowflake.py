import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.snowflake.triggers.snowflake_trigger import SnowflakeSensorTrigger

TASK_ID = "snowflake_check"
POLL_INTERVAL = 1.0
MODULE = "astronomer.providers.snowflake"


class TestSnowflakeSensorTrigger:
    TEST_SQL = "select * from any;"
    TASK_ID = "snowflake_check"

    TRIGGER = SnowflakeSensorTrigger(
        dag_id="unit_test_dag",
        task_id=TASK_ID,
        sql=TEST_SQL,
        poke_interval=POLL_INTERVAL,
        snowflake_conn_id="test_conn",
        run_id=None,
    )

    def test_trigger_serialization(self):
        """
        Asserts that the SnowflakeSensorTrigger correctly serializes its arguments
        and classpath.
        """
        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeSensorTrigger"
        assert kwargs == {
            "task_id": self.TASK_ID,
            "sql": self.TEST_SQL,
            "poke_interval": POLL_INTERVAL,
            "snowflake_conn_id": "test_conn",
            "parameters": None,
            "success": None,
            "failure": None,
            "fail_on_empty": False,
            "dag_id": "unit_test_dag",
            "run_id": None,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "result,return_value,response",
        [
            (
                True,
                {"status": "validate", "message": [[True]]},
                TriggerEvent(
                    {"status": "validate", "result": [[True]], "message": "waiting to validate query"}
                ),
            ),
            (
                None,
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
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.check_query_output")
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.run")
    async def test_snowflake_sensor_trigger_running(
        self,
        mock_hook,
        mock_check_query_output,
        mock_get_query_status,
        result,
        return_value,
        response,
    ):
        """Tests that the SnowflakeTrigger in"""
        mock_get_query_status.return_value = return_value
        mock_check_query_output.return_value = [[True]]

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert response == actual

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
    async def test_snowflake_sensor_trigger_success(self, mock_get_first):
        """Tests that the SnowflakeTrigger in success case"""
        mock_get_first.return_value = {"status": "success"}

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is True
        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.check_query_output")
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.run")
    async def test_snowflake_sensor_trigger_pending(self, mock_run, mock_result):
        """Tests the SnowflakeTrigger does not fire if it reaches a failed state."""
        mock_result.return_value = None

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.run")
    async def test_snowflake_sensor_trigger_exception(self, mock_run, mock_query_status):
        """Tests the SnowflakeSensorTrigger does not fire if there is an exception."""
        mock_query_status.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
