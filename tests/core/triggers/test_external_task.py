import asyncio
from unittest import mock
from unittest.mock import AsyncMock

import asynctest
import pytest
from airflow import AirflowException
from airflow.operators.dummy import DummyOperator
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import DagRunState, TaskInstanceState

from astronomer.providers.core.triggers.external_task import (
    DagStateTrigger,
    ExternalDeploymentTaskTrigger,
    TaskStateTrigger,
)
from astronomer.providers.http.hooks.http import HttpHookAsync
from tests.utils.airflow_util import get_dag_run, get_task_instance
from tests.utils.config import Config


class TestTaskStateTrigger:
    DAG_ID = "external_task"
    TASK_ID = "external_task_op"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]

    @pytest.mark.asyncio
    async def test_task_state_trigger(self, session, dag):
        """
        Asserts that the TaskStateTrigger only goes off on or after a TaskInstance
        reaches an allowed state (i.e. SUCCESS).
        """
        dag_run = get_dag_run(dag.dag_id, self.RUN_ID)
        session.add(dag_run)
        session.commit()

        external_task = DummyOperator(task_id=self.TASK_ID, dag=dag)
        instance = get_task_instance(external_task)
        session.add(instance)
        session.commit()

        trigger = TaskStateTrigger(
            dag_id=dag.dag_id,
            task_id=instance.task_id,
            states=self.STATES,
            execution_dates=[Config.EXECUTION_DATE],
            poll_interval=0.2,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        # Progress the task to a "success" state so that run() yields a TriggerEvent
        instance.state = TaskInstanceState.SUCCESS
        session.commit()
        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = TaskStateTrigger(
            dag_id=self.DAG_ID,
            task_id=self.TASK_ID,
            states=self.STATES,
            execution_dates=[Config.EXECUTION_DATE],
            poll_interval=Config.POLL_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.core.triggers.external_task.TaskStateTrigger"
        assert kwargs == {
            "dag_id": self.DAG_ID,
            "task_id": self.TASK_ID,
            "states": self.STATES,
            "execution_dates": [Config.EXECUTION_DATE],
            "poll_interval": Config.POLL_INTERVAL,
        }


class TestDagStateTrigger:
    DAG_ID = "external_task"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]

    @pytest.mark.asyncio
    async def test_dag_state_trigger(self, session, dag):
        """
        Assert that the DagStateTrigger only goes off on or after a DagRun
        reaches an allowed state (i.e. SUCCESS).
        """
        dag_run = get_dag_run(dag.dag_id, self.RUN_ID)

        session.add(dag_run)
        session.commit()

        trigger = DagStateTrigger(
            dag_id=dag.dag_id,
            states=self.STATES,
            execution_dates=[Config.EXECUTION_DATE],
            poll_interval=0.2,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        # Progress the dag to a "success" state so that yields a TriggerEvent
        dag_run.state = DagRunState.SUCCESS
        session.commit()
        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    def test_serialization(self):
        """Asserts that the DagStateTrigger correctly serializes its arguments and classpath."""
        trigger = DagStateTrigger(
            dag_id=self.DAG_ID,
            states=self.STATES,
            execution_dates=[Config.EXECUTION_DATE],
            poll_interval=Config.POLL_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.core.triggers.external_task.DagStateTrigger"
        assert kwargs == {
            "dag_id": self.DAG_ID,
            "states": self.STATES,
            "execution_dates": [Config.EXECUTION_DATE],
            "poll_interval": Config.POLL_INTERVAL,
        }


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
    @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_task_run_trigger(self, mock_run):
        """Test ExternalDeploymentTaskTrigger is triggered and in running state."""
        mock.AsyncMock(HttpHookAsync)
        mock_run.return_value.json = AsyncMock(return_value={"state": "running"})
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
    @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_task_exception_404(self, mock_run):
        """Test ExternalDeploymentTaskTrigger is triggered and in exception state."""
        mock.AsyncMock(HttpHookAsync)
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
    @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_task_exception(self, mock_run):
        """Test ExternalDeploymentTaskTrigger is triggered and in exception state."""
        mock.AsyncMock(HttpHookAsync)
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
    @asynctest.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
    async def test_deployment_complete(self, mock_run):
        """Assert ExternalDeploymentTaskTrigger runs and complete the run in success state"""
        mock.AsyncMock(HttpHookAsync)
        mock_run.return_value.json = AsyncMock(return_value={"state": "success"})
        trigger = ExternalDeploymentTaskTrigger(
            endpoint=self.TEST_END_POINT,
            http_conn_id=self.CONN_ID,
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"state": "success"}) == actual
