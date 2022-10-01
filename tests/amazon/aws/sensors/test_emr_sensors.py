from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.sensors.emr import (
    EmrContainerSensorAsync,
    EmrJobFlowSensorAsync,
    EmrStepSensorAsync,
)
from astronomer.providers.amazon.aws.triggers.emr import (
    EmrContainerSensorTrigger,
    EmrJobFlowSensorTrigger,
    EmrStepSensorTrigger,
)

TASK_ID = "test_emr_container_sensor"
VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "j-T0CT8Z0C20NT"
AWS_CONN_ID = "aws_default"
STEP_ID = "s-34RJO0CKERRPL"


class TestEmrContainerSensorAsync:
    def test_emr_container_sensor_async(self, context):
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

    def test_emr_container_sensor_async_execute_failure(self, context):
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

    def test_emr_container_sensor_async_execute_complete(self):
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
            task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
            is None
        )

    def test_emr_container_sensor_async_execute_complete_event_none(self):
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


class TestEmrJobFlowSensorAsync:
    def test_emr_job_flow_sensor_async(self, context):
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
        assert isinstance(
            exc.value.trigger, EmrJobFlowSensorTrigger
        ), "Trigger is not a EmrJobFlowSensorTrigger"

    def test_emr_flow_sensor_async_execute_failure(self, context):
        """Test EMR flow sensor with an AirflowException is raised in case of error event"""
        task = EmrJobFlowSensorAsync(
            task_id=TASK_ID,
            job_flow_id=JOB_ID,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

    def test_emr_job_flow_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""
        task = EmrJobFlowSensorAsync(
            task_id=TASK_ID,
            job_flow_id=JOB_ID,
        )
        assert (
            task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
            is None
        )

    def test_emr_job_flow_sensor_async_execute_complete_event_none(self):
        """Asserts that logging occurs as expected"""
        task = EmrJobFlowSensorAsync(
            task_id=TASK_ID,
            job_flow_id=JOB_ID,
        )
        assert task.execute_complete(context=None, event=None) is None


class TestEmrStepSensorAsync:
    def test_emr_step_sensor_async(self, context):
        """Assert execute method defer for EmrStepSensorAsync sensor"""
        task = EmrStepSensorAsync(
            task_id="test_execute",
            job_flow_id=JOB_ID,
            step_id=STEP_ID,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, EmrStepSensorTrigger), "Trigger is not a EmrStepSensorTrigger"

    def test_emr_step_sensor_execute_complete_success(self):
        """Assert execute_complete log success message when triggerer fire with target state"""
        task = EmrStepSensorAsync(
            task_id="test_execute_complete",
            job_flow_id=JOB_ID,
            step_id=STEP_ID,
        )

        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(
                context={}, event={"status": "success", "message": "Job flow currently COMPLETED"}
            )
        mock_log_info.assert_called_with("%s completed successfully.", "j-T0CT8Z0C20NT")

    def test_emr_step_sensor_execute_complete_failure(self):
        """Assert execute_complete method fail"""
        task = EmrStepSensorAsync(
            task_id="test_execute_complete",
            job_flow_id=JOB_ID,
            step_id=STEP_ID,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context={}, event={"status": "error", "message": ""})
