import asyncio
import logging
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.triggers.base import TriggerEvent

from astronomer.providers.databricks.triggers.databricks import DatabricksTrigger

TASK_ID = "databricks_check"
CONN_ID = "databricks_default"
RUN_ID = "1"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLLING_PERIOD_SECONDS = 1.0
MOCK_RUN_RESPONSE_IN_ERROR_1 = {
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "FAILED",
        "state_message": "Test error",
    },
    "tasks": [
        {
            "run_id": 2112892,
            "state": {
                "life_cycle_state": "INTERNAL_ERROR",
                "result_state": "FAILED",
                "user_cancelled_or_timedout": False,
            },
        }
    ],
}
MOCK_RUN_RESPONSE_IN_ERROR_2 = {
    "state": {
        "life_cycle_state": "TERMINATED",
        "result_state": "FAILED",
        "state_message": "Test error",
    },
}
MOCK_RUN_GET_OUTPUT_RESPONSE_IN_ERROR_1 = {
    "error": "Test error",
    "metadata": {
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "FAILED",
            "state_message": "Test error",
        },
        "tasks": [
            {
                "run_id": 2112892,
                "state": {
                    "life_cycle_state": "INTERNAL_ERROR",
                    "result_state": "FAILED",
                    "user_cancelled_or_timedout": False,
                    "state_message": "Test error",
                },
                "run_page_url": "https://my-workspace.cloud.databricks.com/#job/39832/run/20",
            }
        ],
    },
}
MOCK_RUN_GET_OUTPUT_RESPONSE_IN_ERROR_2 = {
    "metadata": {
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "FAILED",
            "state_message": "Test error",
        },
        "tasks": [
            {
                "run_id": 2112892,
                "state": {
                    "life_cycle_state": "INTERNAL_ERROR",
                    "result_state": "FAILED",
                    "user_cancelled_or_timedout": False,
                    "state_message": "Test error",
                },
                "run_page_url": "https://my-workspace.cloud.databricks.com/#job/39832/run/20",
            }
        ],
    }
}


def test_databricks_trigger_serialization():
    """
    Asserts that the DatabricksTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = DatabricksTrigger(
        CONN_ID,
        TASK_ID,
        RUN_ID,
        RETRY_LIMIT,
        RETRY_DELAY,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.databricks.triggers.databricks.DatabricksTrigger"
    assert kwargs == {
        "conn_id": CONN_ID,
        "task_id": TASK_ID,
        "run_id": RUN_ID,
        "retry_limit": 2,
        "retry_delay": 1.0,
        "polling_period_seconds": 1.0,
        "job_id": None,
        "run_page_url": None,
    }


@pytest.mark.asyncio
@mock.patch("airflow.providers.databricks.hooks.databricks.RunState")
@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync.get_run_response")
async def test_databricks_trigger_success(mock_run_response, run_state):
    """
    Tests that the DatabricksTrigger only fires once a
    Databricks run reaches a successful state.
    """
    mock_run_response.return_value = {
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "SUCCESS",
            "state_message": "",
        }
    }
    trigger = DatabricksTrigger(
        conn_id=CONN_ID,
        task_id=TASK_ID,
        run_id=RUN_ID,
        retry_limit=RETRY_LIMIT,
        retry_delay=RETRY_DELAY,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("airflow.providers.databricks.hooks.databricks.RunState")
@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync.get_run_response")
async def test_databricks_trigger_running(mock_run_response, run_state, caplog):
    """
    Tests that the DatabricksTrigger does not fire while a
    Databricks run has not yet reached a terminated state.
    """
    mock_run_response.return_value = {
        "state": {
            "life_cycle_state": "RUNNING",
            "result_state": "",
            "state_message": "In run",
            "user_cancelled_or_timedout": "False",
        }
    }

    caplog.set_level(logging.INFO)

    trigger = DatabricksTrigger(
        conn_id=CONN_ID,
        task_id=TASK_ID,
        run_id=RUN_ID,
        retry_limit=RETRY_LIMIT,
        retry_delay=RETRY_DELAY,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert (
        f"{TASK_ID} in run state: {{'life_cycle_state': 'RUNNING', 'result_state': '', 'state_message': 'In run'}}"
        in caplog.text
    )
    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("airflow.providers.databricks.hooks.databricks.RunState")
@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync.get_run_response")
async def test_databricks_trigger_terminated(mock_run_response, mock_run_state):
    """
    Tests that the DatabricksTrigger does not fire once a
    Databricks run reaches a terminated state.
    Assert that an exception is thrown instead.
    """
    mock_run_response.return_value = {
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "TERMINATED",
            "state_message": "",
        }
    }

    trigger = DatabricksTrigger(
        conn_id=CONN_ID,
        task_id=TASK_ID,
        run_id=RUN_ID,
        retry_limit=RETRY_LIMIT,
        retry_delay=RETRY_DELAY,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert (
        TriggerEvent(
            {
                "status": "error",
                "message": "databricks_check failed with terminal state: {'life_cycle_state': 'TERMINATED',"
                " 'result_state': 'TERMINATED', 'state_message': ''} and with the error ",
            }
        )
        == actual
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_run_response, mock_get_output_response",
    [
        (MOCK_RUN_RESPONSE_IN_ERROR_1, MOCK_RUN_GET_OUTPUT_RESPONSE_IN_ERROR_1),
        (MOCK_RUN_RESPONSE_IN_ERROR_1, MOCK_RUN_GET_OUTPUT_RESPONSE_IN_ERROR_2),
        (MOCK_RUN_RESPONSE_IN_ERROR_2, MOCK_RUN_GET_OUTPUT_RESPONSE_IN_ERROR_2),
    ],
)
@mock.patch("airflow.providers.databricks.hooks.databricks.RunState")
@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync.get_run_output_response")
@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync.get_run_response")
async def test_databricks_trigger_terminal_status_failed(
    mock_run_response_func, mock_get_run_output, mock_run_state, mock_run_response, mock_get_output_response
):
    """
    Tests that the DatabricksTrigger does not fire once a
    Databricks run reaches a terminated state.
    Assert that an exception is thrown instead.
    """
    mock_run_response_func.return_value = mock_run_response
    mock_get_run_output.return_value = mock_get_output_response
    trigger = DatabricksTrigger(
        conn_id=CONN_ID,
        task_id=TASK_ID,
        run_id=RUN_ID,
        retry_limit=RETRY_LIMIT,
        retry_delay=RETRY_DELAY,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert (
        TriggerEvent(
            {
                "status": "error",
                "message": "databricks_check failed with terminal state: {'life_cycle_state': 'TERMINATED',"
                " 'result_state': 'FAILED', 'state_message': 'Test error'}"
                " and with the error Test error",
            }
        )
        == actual
    )


@pytest.mark.asyncio
@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync.get_run_response")
async def test_databricks_trigger_exception(run_state):
    """
    Tests that the DatabricksTrigger yield error in case of exception
    """
    run_state.side_effect = AirflowException("Bad request")

    trigger = DatabricksTrigger(
        conn_id=CONN_ID,
        task_id=TASK_ID,
        run_id=RUN_ID,
        retry_limit=RETRY_LIMIT,
        retry_delay=RETRY_DELAY,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)

    assert actual == TriggerEvent(
        {
            "status": "error",
            "message": "Bad request",
        }
    )
