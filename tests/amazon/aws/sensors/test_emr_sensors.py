from unittest import mock
from unittest.mock import PropertyMock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.sensors.emr import (
    EmrStepSensor,
)

from astronomer.providers.amazon.aws.sensors.emr import (
    EmrContainerSensorAsync,
    EmrJobFlowSensorAsync,
    EmrStepSensorAsync,
)
from astronomer.providers.amazon.aws.triggers.emr import (
    EmrJobFlowSensorTrigger,
)

TASK_ID = "test_emr_container_sensor"
VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "j-T0CT8Z0C20NT"
AWS_CONN_ID = "aws_default"
STEP_ID = "s-34RJO0CKERRPL"

MODULE = "astronomer.providers.amazon.aws.sensors.emr"


MODULE = "astronomer.providers.amazon.aws.sensors.emr"


class TestEmrContainerSensorAsync:
    def test_init(self):
        task = EmrContainerSensorAsync(
            task_id=TASK_ID,
            virtual_cluster_id=VIRTUAL_CLUSTER_ID,
            job_id=JOB_ID,
            poll_interval=5,
            max_retries=1,
            aws_conn_id=AWS_CONN_ID,
        )
        assert isinstance(task, EmrContainerSensorAsync)
        assert task.deferrable is True


class TestEmrStepSensorAsync:
    def test_init(self):
        task = EmrStepSensorAsync(
            task_id="emr_step_sensor",
            job_flow_id=JOB_ID,
            step_id=STEP_ID,
        )
        assert isinstance(task, EmrStepSensor)
        assert task.deferrable is True


class TestEmrJobFlowSensorAsync:
    TASK = EmrJobFlowSensorAsync(
        task_id=TASK_ID,
        job_flow_id=JOB_ID,
    )

    @mock.patch(f"{MODULE}.EmrJobFlowSensorAsync.defer")
    @mock.patch(f"{MODULE}.EmrJobFlowSensorAsync.hook", new_callable=PropertyMock)
    def test_emr_job_flow_sensor_async_finish_before_deferred(self, mock_hook, mock_defer, context):
        """Assert task is not deferred when it receives a finish status before deferring"""
        mock_hook.return_value.conn.describe_cluster.return_value = {
            "Cluster": {"Status": {"State": "TERMINATED"}}
        }
        self.TASK.execute(context)
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.EmrJobFlowSensorAsync.defer")
    @mock.patch(f"{MODULE}.EmrJobFlowSensorAsync.hook", new_callable=PropertyMock)
    def test_emr_job_flow_sensor_async_failed_before_deferred(self, mock_hook, mock_defer, context):
        """Assert task is not deferred when it receives a finish status before deferring"""
        mock_hook.return_value.conn.describe_cluster.return_value = {
            "Cluster": {"Status": {"State": "TERMINATED_WITH_ERRORS"}}
        }
        with pytest.raises(AirflowException):
            self.TASK.execute(context)
        assert not mock_defer.called

    @pytest.mark.parametrize("status", ("STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"))
    @mock.patch(f"{MODULE}.EmrJobFlowSensorAsync.hook", new_callable=PropertyMock)
    def test_emr_job_flow_sensor_async(self, mock_hook, status, context):
        """
        Asserts that a task is deferred and a EmrJobFlowSensorTrigger will be fired
        when the EmrJobFlowSensorAsync is executed.
        """
        mock_hook.return_value.conn.describe_cluster.return_value = {"Cluster": {"Status": {"State": status}}}
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
