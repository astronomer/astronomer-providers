import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterSensorTrigger,
    RedshiftClusterTrigger,
)

TASK_ID = "redshift_trigger_check"
POLLING_PERIOD_SECONDS = 1.0


def test_redshift_cluster_resume_trigger_serialization():
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
    assert classpath == "astronomer.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger"
    assert kwargs == {
        "task_id": TASK_ID,
        "polling_period_seconds": POLLING_PERIOD_SECONDS,
        "aws_conn_id": "test_redshift_conn_id",
        "cluster_identifier": "mock_cluster_identifier",
        "operation_type": "resume_cluster",
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
async def test_redshift_cluster_resume_trigger_success(mock_resume_cluster):
    """
    Test RedshiftClusterTrigger resume cluster with success
    """
    mock_resume_cluster.return_value = {"status": "success", "cluster_state": "available"}
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
    assert task.result() == TriggerEvent({"status": "success", "cluster_state": "available"})
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
async def test_redshift_cluster_resume_trigger_failure(mock_resume_cluster):
    """
    Test RedshiftClusterTrigger resume cluster with failure status
    """
    mock_resume_cluster.return_value = {
        "status": "error",
        "message": "An error occurred (InvalidClusterState) when "
        "calling the ResumeCluster operation: You can only resume a "
        "PAUSED Cluster",
    }
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
    assert task.result() == TriggerEvent(
        {
            "status": "error",
            "message": "An error occurred (InvalidClusterState) when "
            "calling the ResumeCluster operation: You can only resume a "
            "PAUSED Cluster",
        }
    )
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


def test_redshift_pause_resume_trigger_serialization():
    """
    Asserts that the RedshiftClusterTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type="pause_cluster",
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger"
    assert kwargs == {
        "task_id": TASK_ID,
        "polling_period_seconds": POLLING_PERIOD_SECONDS,
        "aws_conn_id": "test_redshift_conn_id",
        "cluster_identifier": "mock_cluster_identifier",
        "operation_type": "pause_cluster",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_result",
    [
        ({"status": "success", "cluster_state": "paused"}),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
async def test_redshift_cluster_pause_trigger_success(mock_pause_cluster, expected_result):
    """
    Tests RedshiftClusterTrigger with pause cluster success
    """
    mock_pause_cluster.return_value = expected_result
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
    assert task.result() == TriggerEvent(expected_result)
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_result",
    [
        ({"status": "success", "cluster_state": "paused"}),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
async def test_redshift_cluster_pause_trigger_failure(mock_pause_cluster, expected_result):
    """
    Test RedshiftClusterTrigger with pause cluster failure
    """
    mock_pause_cluster.return_value = expected_result
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
    assert task.result() == TriggerEvent(expected_result)


def test_redshift_cluster_sensor_trigger_serialization():
    """
    Asserts that the RedshiftClusterTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = RedshiftClusterSensorTrigger(
        task_id=TASK_ID,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        target_status="available",
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert (
        classpath == "astronomer.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterSensorTrigger"
    )
    assert kwargs == {
        "task_id": TASK_ID,
        "aws_conn_id": "test_redshift_conn_id",
        "cluster_identifier": "mock_cluster_identifier",
        "target_status": "available",
        "polling_period_seconds": POLLING_PERIOD_SECONDS,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_result",
    [
        ({"status": "success", "cluster_state": "available"}),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
async def test_redshift_cluster_sensor_trigger_success(mock_cluster_status, expected_result):
    """
    Test RedshiftClusterSensorTrigger with the success status
    """
    mock_cluster_status.return_value = expected_result
    trigger = RedshiftClusterSensorTrigger(
        task_id=TASK_ID,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        target_status="available",
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    assert task.result() == TriggerEvent(expected_result)
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
async def test_redshift_cluster_sensor_trigger_exception(mock_cluster_status):
    """
    Test RedshiftClusterSensorTrigger with exception
    """
    mock_cluster_status.side_effect = Exception("Test exception")
    trigger = RedshiftClusterSensorTrigger(
        task_id=TASK_ID,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        target_status="available",
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    assert task.result() == TriggerEvent(
        {
            "status": "error",
            "message": "Test exception",
        }
    )
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()
