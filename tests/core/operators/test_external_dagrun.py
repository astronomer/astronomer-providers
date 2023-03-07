from datetime import timedelta
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.external_task import (
    ExternalDeploymentTaskSensorAsync,
    ExternalTaskSensorAsync,
)
from astronomer.providers.core.triggers.external_task import (
    DagStateTrigger,
    ExternalDeploymentTaskTrigger,
    TaskStateTrigger,
)


class TestExternalTaskSensorAsync:
    TASK_ID = "external_task_sensor_check"
    EXTERNAL_DAG_ID = "wait_for_me_dag"  # DAG the external task sensor is waiting on
    EXTERNAL_TASK_ID = "wait_for_me_task"  # Task the external task sensor is waiting on

    def test_defer_and_fire_task_state_trigger(self, context):
        """
        Asserts that a task is deferred and an TaskStateTrigger will be fired
        when the ExternalTaskSensor is provided with all required arguments
        (i.e. including the external_task_id).
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_task_id=self.EXTERNAL_TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, TaskStateTrigger), "Trigger is not a TaskStateTrigger"

    def test_defer_and_fire_dag_state_trigger(self, context):
        """
        Asserts that a DAG is deferred and a DagStateTrigger will be fired
        when the ExternalTaskSensor is provided with all required arguments
        (i.e. excluding the external_task_id).
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, DagStateTrigger), "Trigger is not a DagStateTrigger"

    def test_task_defer_when_external_task_id_empty(self, context):
        """
        Asserts that the DagStateTrigger will be fired when the sensor
        is provided with a false value for external_task_id rather than None.
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_task_id="",  # This is a falsely empty string
            external_dag_id=self.EXTERNAL_DAG_ID,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, DagStateTrigger), "Trigger is not a DagStateTrigger"

    @mock.patch("astronomer.providers.core.sensors.external_task.ExternalTaskSensorAsync.get_count")
    def test_execute_complete_when_external_task_fail(self, mocked_count, session, context):
        """
        Asserts that the correct exception is raised when not every task monitored by
        the sensor is executed successfully.
        """
        mocked_count.return_value = 0
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_task_id=self.EXTERNAL_TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
        )

        with pytest.raises(AirflowException) as exc:
            sensor.execute_complete(context, session)

        assert str(exc.value) == "The external task wait_for_me_task in DAG wait_for_me_dag failed."

    @mock.patch("astronomer.providers.core.sensors.external_task.ExternalTaskSensorAsync.get_count")
    def test_execute_complete_when_external_dag_fail(self, mocked_count, session, context):
        """
        Asserts that the correct exception is raised when not every DAG monitored by
        the sensor is executed successfully.
        """
        mocked_count.return_value = 0
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
        )

        with pytest.raises(AirflowException) as exc:
            sensor.execute_complete(context, session)

        assert str(exc.value) == "The external DAG wait_for_me_dag failed."

    def test_get_execution_dates(self, context):
        """
        Asserts that this helper function returns execution_dates as expected
        depending on whether execution_delta, execution_date_fn, or neither
        argument is provided.
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
        )

        # Case #1 sensor.execution_delta only
        sensor.execution_delta = timedelta(days=1)
        execution_dates = sensor.get_execution_dates(context)
        assert execution_dates[0] == datetime(2014, 12, 31)
        assert len(execution_dates) == 1
        sensor.execution_delta = None  # Reset this field for next test

        # Case #2 sensor.execution_date_fn only
        sensor.execution_date_fn = lambda dt: [dt + timedelta(days=i) for i in range(2)]
        execution_dates = sensor.get_execution_dates(context)
        assert execution_dates[0] == datetime(2015, 1, 1)
        assert execution_dates[1] == datetime(2015, 1, 2)
        assert len(execution_dates) == 2
        sensor.execution_date_fn = None  # Reset this field for next test

        # Case #3 both sensor.execution_delta and sensor.execution_date_fn are set to None
        execution_dates = sensor.get_execution_dates(context)
        assert execution_dates[0] == datetime(2015, 1, 1)
        assert len(execution_dates) == 1


class TestExternalDeploymentTaskSensorAsync:
    TASK_ID = "test_external_deployment_task"
    CONN_ID = "http_default"
    ENDPOINT = "test-endpoint/"

    def test_external_deployment_run(self):
        """Assert execute method defer for external deployment task run status sensors"""
        task = ExternalDeploymentTaskSensorAsync(
            task_id=self.TASK_ID,
            http_conn_id=self.CONN_ID,
            endpoint=self.ENDPOINT,
            request_params={},
            headers={},
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute({})
        assert isinstance(
            exc.value.trigger, ExternalDeploymentTaskTrigger
        ), "Trigger is not a ExternalDeploymentTaskTrigger"

    @pytest.mark.parametrize(
        "mock_state, mock_message",
        [("success", "Task Succeeded with response: %s")],
    )
    def test_external_deployment_execute_complete_success(self, mock_state, mock_message):
        """Assert execute_complete log success message when trigger fire with target state"""
        task = ExternalDeploymentTaskSensorAsync(
            task_id=self.TASK_ID,
            http_conn_id=self.CONN_ID,
            endpoint=self.ENDPOINT,
            request_params={},
            headers={},
        )

        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context={}, event={"state": mock_state})
        mock_log_info.assert_called_with(mock_message, {"state": mock_state})

    @pytest.mark.parametrize(
        "mock_resp, mock_message",
        [
            ({"state": "error"}, "Task Failed with response: %s"),
            ({"test": "test"}, "Task Failed with response: %s"),
        ],
    )
    def test_external_deployment_execute_complete_value_error(self, mock_resp, mock_message):
        """Assert execute_complete method to raise Value error"""
        task = ExternalDeploymentTaskSensorAsync(
            task_id=self.TASK_ID,
            http_conn_id=self.CONN_ID,
            endpoint=self.ENDPOINT,
            request_params={},
            headers={},
        )
        with pytest.raises(ValueError):
            task.execute_complete(context={}, event=mock_resp)
