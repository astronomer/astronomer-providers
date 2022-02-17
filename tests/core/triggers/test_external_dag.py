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
import sys

import pytest
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.utils.state import DagRunState

from astronomer_operators.core.triggers.external_task import DagStateTrigger

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
TEST_RUN_ID = "unit_test_dag_run_id"
TEST_STATES = ["success", "fail"]
TEST_POLL_INTERVAL = 3.0


@pytest.mark.skipif(
    sys.version_info.minor <= 6 and sys.version_info.major <= 3,
    reason="No async on 3.6",
)
@pytest.mark.asyncio
async def test_dag_state_trigger(session, dag):
    """
    Tests that the DagStateTrigger only goes off on or after a DagRun
    reaches an allowed state (i.e. SUCCESS).
    """
    dag_run = DagRun(dag.dag_id, run_type="manual", execution_date=DEFAULT_DATE, run_id=TEST_RUN_ID)

    session.add(dag_run)
    session.commit()

    trigger = DagStateTrigger(
        dag.dag_id,
        TEST_STATES,
        [DEFAULT_DATE],
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


def test_task_dag_trigger_serialization():
    """
    Asserts that the DagStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = DagStateTrigger(
        TEST_DAG_ID,
        TEST_STATES,
        [DEFAULT_DATE],
        TEST_POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.core.triggers.external_dag.DagStateTrigger"
    assert kwargs == {
        "dag_id": TEST_DAG_ID,
        "states": TEST_STATES,
        "execution_dates": [DEFAULT_DATE],
        "poll_interval": TEST_POLL_INTERVAL,
    }
