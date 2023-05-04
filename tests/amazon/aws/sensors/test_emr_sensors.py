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

MODULE = "astronomer.providers.amazon.aws.sensors.emr"


class TestEmrContainerSensorAsync:
    TASK = EmrContainerSensorAsync(
        task_id=TASK_ID,
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        poll_interval=5,
        max_retries=1,
        aws_conn_id=AWS_CONN_ID,
    )

    @mock.patch(f"{MODULE}.EmrContainerSensorAsync.defer")
    @mock.patch(f"{MODULE}.EmrContainerSensorAsync.poke", return_value=True)
    def test_emr_container_sensor_async_finish_before_deferred(self, mock_poke, mock_defer, context):
        """Assert task is not deferred when it receives a finish status before deferring"""
        self.TASK.execute(context)
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.EmrContainerSensorAsync.poke", return_value=False)
    def test_emr_container_sensor_async(self, mock_poke, context):
        """
        Asserts that a task is deferred and a EmrContainerSensorTrigger will be fired
        when the EmrContainerSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.TASK.execute(context)
        assert isinstance(
            exc.value.trigger, EmrContainerSensorTrigger
        ), "Trigger is not a EmrContainerSensorTrigger"

    def test_emr_container_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.TASK.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    def test_emr_container_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""

        assert (
            self.TASK.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
            is None
        )

    def test_emr_container_sensor_async_execute_complete_event_none(self):
        """Asserts that logging occurs as expected"""

        assert self.TASK.execute_complete(context=None, event=None) is None


class TestEmrJobFlowSensorAsync:
    TASK = EmrJobFlowSensorAsync(
        task_id=TASK_ID,
        job_flow_id=JOB_ID,
    )

    def test_emr_job_flow_sensor_async(self, context):
        """
        Asserts that a task is deferred and a EmrJobFlowSensorTrigger will be fired
        when the EmrJobFlowSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.TASK.execute(context)
        assert isinstance(
            exc.value.trigger, EmrJobFlowSensorTrigger
        ), "Trigger is not a EmrJobFlowSensorTrigger"

    def test_emr_flow_sensor_async_execute_failure(self, context):
        """Test EMR flow sensor with an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.TASK.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    def test_emr_job_flow_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""

        assert (
            self.TASK.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
            is None
        )

    def test_emr_job_flow_sensor_async_execute_complete_event_none(self):
        """Asserts that logging occurs as expected"""

        assert self.TASK.execute_complete(context=None, event=None) is None


class TestEmrStepSensorAsync:
    TASK = EmrStepSensorAsync(
        task_id="emr_step_sensor",
        job_flow_id=JOB_ID,
        step_id=STEP_ID,
    )

    def test_emr_step_sensor_async(self, context):
        """Assert execute method defer for EmrStepSensorAsync sensor"""

        with pytest.raises(TaskDeferred) as exc:
            self.TASK.execute(context)
        assert isinstance(exc.value.trigger, EmrStepSensorTrigger), "Trigger is not a EmrStepSensorTrigger"

    def test_emr_step_sensor_execute_complete_success(self):
        """Assert execute_complete log success message when triggerer fire with target state"""

        with mock.patch.object(self.TASK.log, "info") as mock_log_info:
            self.TASK.execute_complete(
                context={}, event={"status": "success", "message": "Job flow currently COMPLETED"}
            )
        mock_log_info.assert_called_with("%s completed successfully.", "j-T0CT8Z0C20NT")

    def test_emr_step_sensor_execute_complete_failure(self):
        """Assert execute_complete method fail"""

        with pytest.raises(AirflowException):
            self.TASK.execute_complete(context={}, event={"status": "error", "message": ""})
