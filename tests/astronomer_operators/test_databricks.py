import asyncio
import logging
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.databricks.hooks.databricks import RunState

from astronomer_operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
    DatabricksTrigger,
)

TASK_ID = "databricks_check"
CONN_ID = "databricks_default"
RUN_ID = "1"
RUN_PAGE_URL = "https://www.test.com"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLLING_PERIOD_SECONDS = 1.0
XCOM_RUN_ID_KEY = "run_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
@mock.patch(
    "airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url"
)
def test_databricks_submit_run_operator_async(
    submit_run_response, get_run_page_url_response, context
):
    """
    Asserts that a task is deferred and an DatabricksTrigger will be fired
    when the DatabricksSubmitRunOperatorAsync is executed.
    """
    submit_run_response.return_value = {"run_id": RUN_ID}
    get_run_page_url_response.return_value = RUN_PAGE_URL

    operator = DatabricksSubmitRunOperatorAsync(
        task_id="submit_run",
        databricks_conn_id=CONN_ID,
        existing_cluster_id="xxxx-xxxxxx-xxxxxx",
        notebook_task={
            "notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"
        },
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(
        exc.value.trigger, DatabricksTrigger
    ), "Trigger is not a DatabricksTrigger"


@mock.patch("astronomer_operators.hooks.databricks.DatabricksHook.run_now")
@mock.patch("astronomer_operators.hooks.databricks.DatabricksHook.get_run_page_url")
def test_databricks_run_now_operator_async(
    run_now_response,
    get_run_page_url_response,
):
    """
    Asserts that a task is deferred and an DatabricksTrigger will be fired
    when the DatabricksRunNowOperatorAsync is executed.
    """
    run_now_response.return_value = {"run_id": RUN_ID}
    get_run_page_url_response.return_value = RUN_PAGE_URL

    operator = DatabricksRunNowOperatorAsync(
        task_id="run_now",
        databricks_conn_id=CONN_ID,
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(
        exc.value.trigger, DatabricksTrigger
    ), "Trigger is not a DatabricksTrigger"


@pytest.mark.xfail
def test_databricks_run_now_execute_complete(caplog):
    """
    Asserts that logging occurs as expected.

    TODO: It would appear that logging in the operator is not getting
    picked up by pytest right now... come back to this later.
    """
    caplog.set_level(logging.INFO)
    operator = DatabricksRunNowOperatorAsync(
        task_id=TASK_ID,
        databricks_conn_id=CONN_ID,
    )
    operator.run_page_url = RUN_PAGE_URL
    operator.execute_complete({})

    assert f"{TASK_ID} completed successfully." in caplog.text


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
    assert classpath == "astronomer_operators.databricks.DatabricksTrigger"
    assert kwargs == {
        "conn_id": CONN_ID,
        "task_id": TASK_ID,
        "run_id": RUN_ID,
        "retry_limit": 2,
        "retry_delay": 1.0,
        "polling_period_seconds": 1.0,
    }


@pytest.mark.asyncio
@mock.patch(
    "astronomer_operators.hooks.databricks.DatabricksHookAsync.get_run_state_async"
)
async def test_databricks_trigger_success(run_state):
    """
    Tests that the DatabricksTrigger only fires once a
    Databricks run reaches a successful state.
    """
    run_state.return_value = RunState(
        life_cycle_state="TERMINATED",
        result_state="SUCCESS",
        state_message="",
    )
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
@mock.patch(
    "astronomer_operators.hooks.databricks.DatabricksHookAsync.get_run_state_async"
)
async def test_databricks_trigger_running(run_state, caplog):
    """
    Tests that the DatabricksTrigger does not fire while a
    Databricks run has not yet reached a terminated state.
    """
    run_state.return_value = RunState(
        life_cycle_state="RUNNING",
        result_state="",
        state_message="In run",
    )

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
@mock.patch(
    "astronomer_operators.hooks.databricks.DatabricksHookAsync.get_run_state_async"
)
async def test_databricks_trigger_terminated(run_state):
    """
    Tests that the DatabricksTrigger does not fire once a
    Databricks run reaches a terminated state.
    Assert that an exception is thrown instead.
    """
    run_state.return_value = RunState(
        life_cycle_state="TERMINATED",
        result_state="TERMINATED",
        state_message="",
    )

    trigger = DatabricksTrigger(
        conn_id=CONN_ID,
        task_id=TASK_ID,
        run_id=RUN_ID,
        retry_limit=RETRY_LIMIT,
        retry_delay=RETRY_DELAY,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    with pytest.raises(AirflowException) as exc:
        await trigger.run().__anext__()

    assert (
        str(exc.value)
        == f"{TASK_ID} failed with terminal state: {run_state.return_value}"
    )
