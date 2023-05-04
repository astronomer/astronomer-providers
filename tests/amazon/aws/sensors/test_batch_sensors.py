from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.sensors.batch import BatchSensorAsync
from astronomer.providers.amazon.aws.triggers.batch import BatchSensorTrigger

MODULE = "astronomer.providers.amazon.aws.sensors.batch"


class TestBatchSensorAsync:
    JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
    AWS_CONN_ID = "airflow_test"
    REGION_NAME = "eu-west-1"
    TASK = BatchSensorAsync(
        task_id="task",
        job_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION_NAME,
    )

    @mock.patch(f"{MODULE}.BatchSensorAsync.defer")
    @mock.patch(f"{MODULE}.BatchSensorAsync.poke", return_value=True)
    def test_batch_sensor_async_finish_before_deferred(self, mock_poke, mock_defer, context):
        """Assert task is not deferred when it receives a finish status before deferring"""
        self.TASK.execute(context)
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.BatchSensorAsync.poke", return_value=False)
    def test_batch_sensor_async(self, context):
        """
        Asserts that a task is deferred and a BatchSensorTrigger will be fired
        when the BatchSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.TASK.execute(context)
        assert isinstance(exc.value.trigger, BatchSensorTrigger), "Trigger is not a BatchSensorTrigger"

    def test_batch_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException) as exc_info:
            self.TASK.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

        assert str(exc_info.value) == "test failure message"

    @pytest.mark.parametrize(
        "event",
        [{"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"}],
    )
    def test_batch_sensor_async_execute_complete(self, caplog, event):
        """Tests that execute_complete method returns None and that it prints expected log"""

        with mock.patch.object(self.TASK.log, "info") as mock_log_info:
            assert self.TASK.execute_complete(context=None, event=event) is None

        mock_log_info.assert_called_with(event["message"])

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for BatchSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            BatchSensorAsync(
                task_id="task",
                job_id=self.JOB_ID,
                aws_conn_id=self.AWS_CONN_ID,
                region_name=self.REGION_NAME,
                poll_interval=5.0,
            )
