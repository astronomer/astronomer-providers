import asyncio
from unittest import mock

import pytest
from airflow import AirflowException
from airflow.providers.http.hooks.http import HttpAsyncHook
from airflow.triggers.base import TriggerEvent

from astronomer.providers.core.triggers.external_task import ExternalDeploymentTaskTrigger


class TestExternalDeploymentTaskTrigger:
    TEST_END_POINT = "test-endpoint"
    CONN_ID = "http_default"

    def test_deployment_task_trigger_serialization(self):
        """
        Asserts that the ExternalDeploymentTaskTrigger correctly serializes its arguments and classpath.
        """
        trigger = ExternalDeploymentTaskTrigger(
            endpoint=self.TEST_END_POINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.core.triggers.external_task.ExternalDeploymentTaskTrigger"
        assert kwargs == {
            "data": None,
            "endpoint": self.TEST_END_POINT,
            "extra_options": {},
            "headers": {"Content-Type": "application/json"},
            "http_conn_id": self.CONN_ID,
            "poke_interval": 5.0,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.http.hooks.http.HttpAsyncHook.run")
    async def test_deployment_task_run_trigger(self, mock_run):
        """Test ExternalDeploymentTaskTrigger is triggered and in running state."""
        mock.Mock(HttpAsyncHook)
        mock_run.return_value.json = mock.AsyncMock(return_value={"state": "running"})
        trigger = ExternalDeploymentTaskTrigger(
            endpoint=self.TEST_END_POINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.http.hooks.http.HttpAsyncHook.run")
    async def test_deployment_task_exception_404(self, mock_run):
        """Test ExternalDeploymentTaskTrigger is triggered and in exception state."""
        mock.Mock(HttpAsyncHook)
        mock_run.side_effect = AirflowException("404 test error")
        trigger = ExternalDeploymentTaskTrigger(
            endpoint=self.TEST_END_POINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.http.hooks.http.HttpAsyncHook.run")
    async def test_deployment_task_exception(self, mock_run):
        """Test ExternalDeploymentTaskTrigger is triggered and in exception state."""
        mock.Mock(HttpAsyncHook)
        mock_run.side_effect = AirflowException("Test exception")
        trigger = ExternalDeploymentTaskTrigger(
            endpoint=self.TEST_END_POINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"state": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.http.hooks.http.HttpAsyncHook.run")
    async def test_deployment_complete(self, mock_run):
        """Assert ExternalDeploymentTaskTrigger runs and complete the run in success state"""
        mock.Mock(HttpAsyncHook)
        mock_run.return_value.json = mock.AsyncMock(return_value={"state": "success"})
        trigger = ExternalDeploymentTaskTrigger(
            endpoint=self.TEST_END_POINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"state": "success"}) == actual
