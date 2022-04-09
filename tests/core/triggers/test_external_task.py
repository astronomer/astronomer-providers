import asyncio
import sys

import pytest
from airflow.models import DagRun, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

from astronomer.providers.core.triggers.external_task import TaskStateTrigger

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_dag"
TEST_TASK_ID = "external_task_sensor_check"
TEST_RUN_ID = "unit_test_dag_run_id"
TEST_STATES = ["success", "fail"]
TEST_POLL_INTERVAL = 3.0


@pytest.mark.skipif(
    sys.version_info.minor <= 6 and sys.version_info.major <= 3,
    reason="No async on 3.6",
)
@pytest.mark.asyncio
async def test_task_state_trigger(session, dag):
    """
    Asserts that the TaskStateTrigger only goes off on or after a TaskInstance
    reaches an allowed state (i.e. SUCCESS).
    """
    dag_run = DagRun(dag.dag_id, run_type="manual", execution_date=DEFAULT_DATE, run_id=TEST_RUN_ID)

    session.add(dag_run)
    session.commit()

    external_task = DummyOperator(task_id=TEST_TASK_ID, dag=dag)
    instance = TaskInstance(external_task, DEFAULT_DATE)
    session.add(instance)
    session.commit()

    trigger = TaskStateTrigger(
        dag.dag_id,
        instance.task_id,
        TEST_STATES,
        [DEFAULT_DATE],
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


def test_task_state_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = TaskStateTrigger(
        TEST_DAG_ID,
        TEST_TASK_ID,
        TEST_STATES,
        [DEFAULT_DATE],
        TEST_POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.core.triggers.external_task.TaskStateTrigger"
    assert kwargs == {
        "dag_id": TEST_DAG_ID,
        "task_id": TEST_TASK_ID,
        "states": TEST_STATES,
        "execution_dates": [DEFAULT_DATE],
        "poll_interval": TEST_POLL_INTERVAL,
    }
