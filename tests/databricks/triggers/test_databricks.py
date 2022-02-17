# Copyright 2022 Astronomer Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
from unittest import mock

import pytest
from airflow import AirflowException
from airflow.providers.databricks.hooks.databricks import RunState

from astronomer_operators.databricks.triggers.databricks import DatabricksTrigger

TASK_ID = "databricks_check"
CONN_ID = "databricks_default"
RUN_ID = "1"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLLING_PERIOD_SECONDS = 1.0


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
    assert classpath == "astronomer_operators.databricks.triggers.databricks.DatabricksTrigger"
    assert kwargs == {
        "conn_id": CONN_ID,
        "task_id": TASK_ID,
        "run_id": RUN_ID,
        "retry_limit": 2,
        "retry_delay": 1.0,
        "polling_period_seconds": 1.0,
    }


@pytest.mark.asyncio
@mock.patch("astronomer_operators.databricks.hooks.databricks.DatabricksHookAsync.get_run_state_async")
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
@mock.patch("astronomer_operators.databricks.hooks.databricks.DatabricksHookAsync.get_run_state_async")
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
@mock.patch("astronomer_operators.databricks.hooks.databricks.DatabricksHookAsync.get_run_state_async")
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

    assert str(exc.value) == f"{TASK_ID} failed with terminal state: {run_state.return_value}"
