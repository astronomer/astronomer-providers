import asyncio
from unittest import mock
from unittest.mock import AsyncMock

import asynctest
import pytest
from airflow import AirflowException
from airflow.triggers.base import TriggerEvent

from astronomer.providers.http.hooks.http import HttpHookAsync
from astronomer.providers.http.triggers.http import (
    ExternalDeploymentTaskTrigger,
    HttpTrigger,
)


def test_http_trigger_serialization():
    """
    Asserts that the HttpTrigger correctly serializes its arguments and classpath.
    """
    trigger = HttpTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
        method="GET",
        headers={"Content-Type": "application/json"},
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.http.triggers.http.HttpTrigger"
    assert kwargs == {
        "data": None,
        "endpoint": "test-endpoint",
        "extra_options": {},
        "headers": {"Content-Type": "application/json"},
        "http_conn_id": "http_default",
        "poke_interval": 5.0,
    }


def test_deployment_task_trigger_serialization():
    """
    Asserts that the ExternalDeploymentTaskTrigger correctly serializes its arguments and classpath.
    """
    trigger = ExternalDeploymentTaskTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
        method="GET",
        headers={"Content-Type": "application/json"},
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.http.triggers.http.ExternalDeploymentTaskTrigger"
    assert kwargs == {
        "data": None,
        "endpoint": "test-endpoint",
        "extra_options": {},
        "headers": {"Content-Type": "application/json"},
        "http_conn_id": "http_default",
        "poke_interval": 5.0,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
async def test_deployment_task_run_trigger(mock_run):
    """Test ExternalDeploymentTaskTrigger is triggered and in running state."""
    mock.AsyncMock(HttpHookAsync)
    mock_run.return_value.json = AsyncMock(return_value={"state": "running"})
    trigger = ExternalDeploymentTaskTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
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
async def test_deployment_task_exception_404(mock_run):
    """Test ExternalDeploymentTaskTrigger is triggered and in exception state."""
    mock.AsyncMock(HttpHookAsync)
    mock_run.side_effect = AirflowException("404 test error")
    trigger = ExternalDeploymentTaskTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
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
async def test_deployment_task_exception(mock_run):
    """Test ExternalDeploymentTaskTrigger is triggered and in exception state."""
    mock.AsyncMock(HttpHookAsync)
    mock_run.side_effect = AirflowException("Test exception")
    trigger = ExternalDeploymentTaskTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
        method="GET",
        headers={"Content-Type": "application/json"},
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"state": "error", "message": "Test exception"}) == actual


@pytest.mark.asyncio
@asynctest.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
async def test_deployment_complete(mock_run):
    """Assert ExternalDeploymentTaskTrigger runs and complete the run in success state"""
    mock.AsyncMock(HttpHookAsync)
    mock_run.return_value.json = AsyncMock(return_value={"state": "success"})
    trigger = ExternalDeploymentTaskTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
        method="GET",
        headers={"Content-Type": "application/json"},
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"state": "success"}) == actual
