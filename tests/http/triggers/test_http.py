# import asyncio
# from unittest import mock
# from unittest.mock import AsyncMock
#
# import pytest
# from airflow.triggers.base import TriggerEvent
# from asynctest import CoroutineMock

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
        "poll_interval": 5.0,
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
        "poll_interval": 5.0,
    }


# @pytest.mark.asyncio
# @mock.patch("astronomer.providers.http.triggers.http.ExternalDeploymentTaskTrigger._get_async_hook")
# @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync.run")
# async def test_deployment_task_run_exception(mock_run, mock_get_async_hook):
#     """Assert that run catch exception if dbt cloud job API throw exception"""
#     mock_run.side_effect = Exception("Test exception")
#     trigger = ExternalDeploymentTaskTrigger(
#         endpoint="test-endpoint",
#         http_conn_id="http_default",
#         method="GET",
#         headers={"Content-Type": "application/json"},
#     )
#     task = [i async for i in trigger.run()]
#     response = TriggerEvent(
#         {
#             "state": "error",
#             "message": "Test exception",
#         }
#     )
#     assert len(task) == 1
#     assert response in task


# @pytest.mark.asyncio
# @mock.patch("astronomer.providers.http.triggers.http.ExternalDeploymentTaskTrigger._get_async_hook")
# @mock.patch("astronomer.providers.http.hooks.http.HttpHookAsync")
# @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.aiohttp.ClientSession.get")
# async def test_deployment_task_run_exception(mock_get, mock_run, mock_get_async_hook):
#     """Assert that run catch exception if dbt cloud job API throw exception"""
#     # returned_data = {"key": "value"}
#     # mock_run.return_value.__aenter__.return_value.json = CoroutineMock(
#     #     side_effect=lambda: returned_data
#     # )
#     # mock_run.return_value.__aenter__.return_value.json = AsyncMock(return_value={"state": "success"})
#     mock_run.return_value.run.return_value = AsyncMock(return_value={"state": "success"})
#     trigger = ExternalDeploymentTaskTrigger(
#         endpoint="test-endpoint",
#         http_conn_id="http_default",
#         method="GET",
#         headers={"Content-Type": "application/json"},
#     )
#     generator = trigger.run()
#     actual = await generator.asend(None)
#     assert TriggerEvent({"state": "success"}) == actual
