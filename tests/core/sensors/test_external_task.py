from datetime import timedelta
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from astronomer.providers.core.triggers.external_task import (
    DagStateTrigger,
    TaskStateTrigger,
)


class TestExternalTaskSensorAsync:
    TASK_ID = "external_task_sensor_check"
    EXTERNAL_DAG_ID = "wait_for_me_dag"  # DAG the external task sensor is waiting on
    EXTERNAL_TASK_ID = "wait_for_me_task"  # Task the external task sensor is waiting on

    def test_defer_and_fire_task_state_trigger(self, dag, context):
        """
        Asserts that a task is deferred and an TaskStateTrigger will be fired
        when the ExternalTaskSensor is provided with all required arguments
        (i.e. including the external_task_id).
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_task_id=self.EXTERNAL_TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
            dag=dag,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, TaskStateTrigger), "Trigger is not a TaskStateTrigger"

    def test_defer_and_fire_dag_state_trigger(self, dag, context):
        """
        Asserts that a DAG is deferred and a DagStateTrigger will be fired
        when the ExternalTaskSensor is provided with all required arguments
        (i.e. excluding the external_task_id).
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
            dag=dag,
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, DagStateTrigger), "Trigger is not a DagStateTrigger"

    def test_task_defer_when_external_task_id_empty(self, dag, context):
        """
        Asserts that the DagStateTrigger will be fired when the sensor
        is provided with a falsy value for external_task_id rather than None.
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_task_id="",  # This is a falsy empty string
            external_dag_id=self.EXTERNAL_DAG_ID,
            dag=dag,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, DagStateTrigger), "Trigger is not a DagStateTrigger"

    @mock.patch("astronomer.providers.core.sensors.external_task.ExternalTaskSensorAsync.get_count")
    def test_execute_complete_when_external_task_fail(self, mocked_count, session, dag, context):
        """
        Asserts that the correct exception is raised when not every task monitored by
        the sensor is executed successfully.
        """
        mocked_count.return_value = 0
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_task_id=self.EXTERNAL_TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
            dag=dag,
        )

        with pytest.raises(AirflowException) as exc:
            sensor.execute_complete(context, session)

        assert str(exc.value) == "The external task wait_for_me_task in DAG wait_for_me_dag failed."

    @mock.patch("astronomer.providers.core.sensors.external_task.ExternalTaskSensorAsync.get_count")
    def test_execute_complete_when_external_dag_fail(self, mocked_count, session, dag, context):
        """
        Asserts that the correct exception is raised when not every DAG monitored by
        the sensor is executed successfully.
        """
        mocked_count.return_value = 0
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
            dag=dag,
        )

        with pytest.raises(AirflowException) as exc:
            sensor.execute_complete(context, session)

        assert str(exc.value) == "The external DAG wait_for_me_dag failed."

    def test_get_execution_dates(self, dag, context):
        """
        Asserts that this helper function returns execution_dates as expected
        depending on whether execution_delta, execution_date_fn, or neither
        argument is provided.
        """
        sensor = ExternalTaskSensorAsync(
            task_id=self.TASK_ID,
            external_dag_id=self.EXTERNAL_DAG_ID,
            dag=dag,
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
