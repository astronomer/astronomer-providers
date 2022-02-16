import asyncio
from unittest import mock

import pytest

from astronomer_operators.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterTrigger,
)

TASK_ID = "redshift_trigger_check"
POLLING_PERIOD_SECONDS = 1.0


def test_snowflake_trigger_serialization():
    """
    Asserts that the RedshiftClusterTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type="resume_cluster",
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger"
    assert kwargs == {
        "task_id": TASK_ID,
        "polling_period_seconds": POLLING_PERIOD_SECONDS,
        "aws_conn_id": "test_redshift_conn_id",
        "cluster_identifier": "mock_cluster_identifier",
        "operation_type": "resume_cluster",
    }


@pytest.mark.asyncio
@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
async def test_redshift_cluster_resume_trigger_success(mock_resume_cluster):
    """
    Tests RedshiftClusterTrigger
    """
    mock_resume_cluster.return_value = "pausing"
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type="resume_cluster",
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    print(task.result())
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
async def test_redshift_cluster_pause_trigger_success(mock_pause_cluster):
    """
    Tests RedshiftClusterTrigger to test the pause status
    """
    mock_pause_cluster.return_value = "pausing"
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type="pause_cluster",
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    print(task.result())
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()
