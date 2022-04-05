import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.sensors.emr import (
    EmrContainerSensorAsync,
    EmrJobFlowSensorAsync,
)
from astronomer.providers.amazon.aws.triggers.emr import (
    EmrContainerSensorTrigger,
    EmrJobFlowSensorTrigger,
)

TASK_ID = "test_emr_container_sensor"
VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"


@pytest.fixture()
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_emr_container_sensor_async():
    """
    Asserts that a task is deferred and a EmrContainerSensorTrigger will be fired
    when the EmrContainerSensorAsync is executed.
    """
    task = EmrContainerSensorAsync(
        task_id=TASK_ID,
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        poll_interval=5,
        max_retries=1,
        aws_conn_id=AWS_CONN_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(
        exc.value.trigger, EmrContainerSensorTrigger
    ), "Trigger is not a EmrContainerSensorTrigger"


def test_emr_container_sensor_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = EmrContainerSensorAsync(
        task_id=TASK_ID,
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        poll_interval=5,
        max_retries=1,
        aws_conn_id=AWS_CONN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_emr_container_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = EmrContainerSensorAsync(
        task_id=TASK_ID,
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        poll_interval=5,
        max_retries=1,
        aws_conn_id=AWS_CONN_ID,
    )
    assert (
        task.execute_complete(context=None, event={"status": "success", "message": "Job completed"}) is None
    )


def test_emr_container_sensor_async_execute_complete_event_none():
    """Asserts that logging occurs as expected"""
    task = EmrContainerSensorAsync(
        task_id=TASK_ID,
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        poll_interval=5,
        max_retries=1,
        aws_conn_id=AWS_CONN_ID,
    )
    assert task.execute_complete(context=None, event=None) is None


def test_emr_job_flow_sensor_async():
    """
    Asserts that a task is deferred and a EmrJobFlowSensorTrigger will be fired
    when the EmrJobFlowSensorAsync is executed.
    """
    task = EmrJobFlowSensorAsync(
        task_id=TASK_ID,
        job_flow_id=JOB_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, EmrJobFlowSensorTrigger), "Trigger is not a EmrJobFlowSensorTrigger"


def test_emr_flow_sensor_async_execute_failure(context):
    """Test EMR flow sensor with an AirflowException is raised in case of error event"""
    task = EmrJobFlowSensorAsync(
        task_id=TASK_ID,
        job_flow_id=JOB_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_emr_job_flow_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = EmrJobFlowSensorAsync(
        task_id=TASK_ID,
        job_flow_id=JOB_ID,
    )
    assert (
        task.execute_complete(context=None, event={"status": "success", "message": "Job completed"}) is None
    )


def test_emr_job_flow_sensor_async_execute_complete_event_none():
    """Asserts that logging occurs as expected"""
    task = EmrJobFlowSensorAsync(
        task_id=TASK_ID,
        job_flow_id=JOB_ID,
    )
    assert task.execute_complete(context=None, event=None) is None
