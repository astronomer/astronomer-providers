import asyncio
from unittest import mock
from unittest.mock import AsyncMock

import asynctest
import pytest
from airflow import AirflowException
from airflow.triggers.base import TriggerEvent

from astronomer.providers.core.triggers.external_dagrun import ExternalDeploymentDagRunTrigger
from astronomer.providers.http.hooks.http import HttpHookAsync

DAGRUN_ENDPOINT = "/api/v1/dags/{dag_id}/dagRuns/{run_id}"


class TestExternalDeploymentDagRunTrigger:
    DAG_ID = "test-dag"
    RUN_ID = "test-run"
    CONN_ID = "http_default"

    ENDPOINT = DAGRUN_ENDPOINT.format(dag_id=DAG_ID, run_id=RUN_ID)
    HEADERS = {"Content-Type": "application/json"}

    def test_deployment_dag_run_trigger_serialization(self):
        """
        Asserts that the ExternalDeploymentDagRunTrigger correctly serializes its arguments and classpath.
        """
        trigger = ExternalDeploymentDagRunTrigger(
            dag_id=self.DAG_ID,
            run_id=self.RUN_ID,
            http_conn_id=self.CONN_ID,
            method="GET",
            endpoint=self.ENDPOINT,
            data=None,
            headers=self.HEADERS,
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath == "astronomer.providers.core.triggers.external_dagrun.ExternalDeploymentDagRunTrigger"
        )
        assert kwargs == {
            "data": None,
            "endpoint": self.ENDPOINT,
            "extra_options": {},
            "headers": self.HEADERS,
            "http_conn_id": self.CONN_ID,
            "poke_interval": 5.0,
            "dag_id": "test-dag",
            "run_id": "test-run",
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_dag_run_run_trigger(self, mock_run):
        """Test ExternalDeploymentDagRunTrigger is triggered and in running state."""
        mock.AsyncMock(HttpHookAsync)
        mock_run.return_value.json = AsyncMock(return_value={"state": "running"})
        trigger = ExternalDeploymentDagRunTrigger(
            dag_id=self.DAG_ID,
            run_id=self.RUN_ID,
            endpoint=self.ENDPOINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers=self.HEADERS,
        )
        dag_run = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert dag_run.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_dag_run_exception_404(self, mock_run):
        """Test ExternalDeploymentDagRunTrigger is triggered and in exception state."""
        mock.AsyncMock(HttpHookAsync)
        mock_run.side_effect = AirflowException("404 test error")
        trigger = ExternalDeploymentDagRunTrigger(
            dag_id=self.DAG_ID,
            run_id=self.RUN_ID,
            endpoint=self.ENDPOINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers=self.HEADERS,
        )
        dag_run = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert dag_run.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_dag_run_exception(self, mock_run):
        """Test ExternalDeploymentDagRunTrigger is triggered and in exception state."""
        mock.AsyncMock(HttpHookAsync)
        mock_run.side_effect = AirflowException("Test exception")
        trigger = ExternalDeploymentDagRunTrigger(
            dag_id=self.DAG_ID,
            run_id=self.RUN_ID,
            endpoint=self.ENDPOINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers=self.HEADERS,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"state": "error", "message": "Test exception"}) == actual

    @pytest.mark.asyncio
    @asynctest.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_complete(self, mock_run):
        """Assert ExternalDeploymentDagRunTrigger runs and complete the run in success state"""
        mock.AsyncMock(HttpHookAsync)
        mock_run.return_value.json = AsyncMock(return_value={"state": "success"})
        trigger = ExternalDeploymentDagRunTrigger(
            dag_id=self.DAG_ID,
            run_id=self.RUN_ID,
            endpoint=self.ENDPOINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers=self.HEADERS,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"state": "success"}) == actual
