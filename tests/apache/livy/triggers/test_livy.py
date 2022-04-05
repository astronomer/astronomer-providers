import asyncio
from unittest import mock

import pytest
from aiohttp import ClientConnectionError
from airflow.providers.apache.livy.hooks.livy import BatchState
from airflow.triggers.base import TriggerEvent

from astronomer.providers.apache.livy.triggers.livy import LivyTrigger


def test_livy_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=0)
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.apache.livy.triggers.livy.LivyTrigger"
    assert kwargs == {
        "batch_id": 1,
        "spark_params": {},
        "livy_conn_id": "livy_default",
        "polling_interval": 0,
        "extra_options": None,
        "extra_headers": None,
        "livy_hook_async": None,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.triggers.livy.LivyTrigger.poll_for_termination")
async def test_livy_trigger_run_with_no_poll_interval(mock_poll_for_termination):
    """
    Test if the task ran in the triggerer successfully with poll interval=0. In the case when polling_interval=0, it
    should return the batch_id
    :return:
    """
    mock_poll_for_termination.return_value = {"status": "success"}
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=0)
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert (
        TriggerEvent({"status": "success", "batch_id": 1, "response": "Batch 1 succeeded", "log_lines": None})
        in task
    )


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.triggers.livy.LivyTrigger.poll_for_termination")
async def test_livy_trigger_run_with_poll_interval_success(mock_poll_for_termination):
    """
    Test if the task ran in the triggerer successfully with poll interval>0. In the case when polling_interval > 0,
    it should return a success or failure status.
    :return:
    """
    mock_poll_for_termination.return_value = {"status": "success"}
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=30)

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "success"}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.triggers.livy.LivyTrigger.poll_for_termination")
async def test_livy_trigger_run_with_poll_interval_error(mock_poll_for_termination):
    """
    Test if the task in the triggerer returned an error when poll_for_termination returned error.
    :return:
    """
    mock_poll_for_termination.return_value = {"status": "error"}
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=30)

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error"}) in task


@pytest.mark.asyncio
async def test_livy_trigger_run_with_exception():
    """
    Test if the task in the triggerer failed with a connection error when no connection is mocked.
    :return:
    """
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=30)

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert (
        TriggerEvent(
            {
                "status": "error",
                "batch_id": 1,
                "response": "Batch 1 did not succeed with Cannot connect to host livy:8998 ssl:default "
                "[Name or service not known]",
                "log_lines": None,
            }
        )
        in task
    )


@pytest.mark.asyncio
async def test_livy_trigger_poll_for_termination_with_client_error():
    """
    Test if the poll_for_termination() in the triggerer failed with a ClientConnectionError when no connection is
    mocked.
    :return:
    """
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=30)

    with pytest.raises(ClientConnectionError):
        await trigger.poll_for_termination(1)


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_state")
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.dump_batch_logs")
async def test_livy_trigger_poll_for_termination_success(mock_dump_batch_logs, mock_get_batch_state):
    """
    Test if the poll_for_termination() in the triggerer returned success response when get_batch_state() runs
    successfully.
    :return:
    """
    mock_get_batch_state.return_value = {"batch_state": BatchState.SUCCESS}
    mock_dump_batch_logs.return_value = ["mock_log"]
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=30)

    task = await trigger.poll_for_termination(1)

    assert task == {
        "status": "success",
        "batch_id": 1,
        "response": "Batch 1 succeeded",
        "log_lines": ["mock_log"],
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_state")
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.dump_batch_logs")
async def test_livy_trigger_poll_for_termination_error(mock_dump_batch_logs, mock_get_batch_state):
    """
    Test if the poll_for_termination() in the triggerer returned error response when get_batch_state() failed.
    :return:
    """
    mock_get_batch_state.return_value = {"batch_state": BatchState.ERROR}
    mock_dump_batch_logs.return_value = ["mock_log"]
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=30)

    task = await trigger.poll_for_termination(1)

    assert task == {
        "status": "error",
        "batch_id": 1,
        "response": "Batch 1 did not succeed",
        "log_lines": ["mock_log"],
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_state")
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.dump_batch_logs")
async def test_livy_trigger_poll_for_termination_state(mock_dump_batch_logs, mock_get_batch_state):
    """
    Test if the poll_for_termination() in the triggerer is still polling when get_batch_state() returned
    NOT_STARTED.
    :return:
    """
    mock_get_batch_state.return_value = {"batch_state": BatchState.NOT_STARTED}
    mock_dump_batch_logs.return_value = ["mock_log"]
    trigger = LivyTrigger(batch_id=1, spark_params={}, livy_conn_id="livy_default", polling_interval=30)

    task = asyncio.create_task(trigger.poll_for_termination(1))
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()
