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
@pytest.mark.parametrize(
    "operation_type,return_value,response",
    [
        (
            "resume_cluster",
            {"status": "error", "message": "test error"},
            TriggerEvent({"status": "error", "message": "test error"}),
        ),
        (
            "resume_cluster",
            {"status": "success", "cluster_state": "available"},
            TriggerEvent({"status": "success", "cluster_state": "available"}),
        ),
        ("resume_cluster", None, TriggerEvent({"status": "error", "message": f"{TASK_ID} failed"})),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
async def test_redshift_cluster_resume_trigger_run(
    mock_resume_cluster, operation_type, return_value, response
):
    """Test RedshiftClusterTrigger resume cluster with success"""
    mock_resume_cluster.return_value = return_value
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type=operation_type,
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert response == actual


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
async def test_redshift_cluster_resume_trigger_failure(mock_resume_cluster):
    """Test RedshiftClusterTrigger resume cluster with failure status"""
    mock_resume_cluster.side_effect = Exception("Test exception")
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type="resume_cluster",
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


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
    "operation_type,return_value,response",
    [
        (
            "pause_cluster",
            {"status": "error", "message": "test error"},
            TriggerEvent({"status": "error", "message": "test error"}),
        ),
        (
            "pause_cluster",
            {"status": "success", "cluster_state": "paused"},
            TriggerEvent({"status": "success", "cluster_state": "paused"}),
        ),
        ("pause_cluster", False, TriggerEvent({"status": "error", "message": f"{TASK_ID} failed"})),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
async def test_redshift_cluster_pause_trigger_run(mock_pause_cluster, operation_type, return_value, response):
    """
    Test RedshiftClusterTrigger resume cluster with success
    """
    mock_pause_cluster.return_value = return_value
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type=operation_type,
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert response == actual


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
async def test_redshift_cluster_pause_trigger_failure(mock_pause_cluster):
    """Test RedshiftClusterTrigger with pause cluster failure"""
    mock_pause_cluster.side_effect = Exception("Test exception")
    trigger = RedshiftClusterTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        operation_type="pause_cluster",
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


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

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent(expected_result) == actual


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_result",
    [
        ({"status": "success", "cluster_state": "Resuming"}),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
async def test_redshift_cluster_sensor_trigger_resuming_status(mock_cluster_status, expected_result):
    """Test RedshiftClusterSensorTrigger with the success status"""
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
    assert task.done() is False

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
async def test_redshift_cluster_sensor_trigger_exception(mock_cluster_status):
    """Test RedshiftClusterSensorTrigger with exception"""
    mock_cluster_status.side_effect = Exception("Test exception")
    trigger = RedshiftClusterSensorTrigger(
        task_id=TASK_ID,
        aws_conn_id="test_redshift_conn_id",
        cluster_identifier="mock_cluster_identifier",
        target_status="available",
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

    task = [i async for i in trigger.run()]
    # since we use return as soon as we yield the trigger event
    # at any given point there should be one trigger event returned to the task
    # so we validate for length of task to be 1
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
