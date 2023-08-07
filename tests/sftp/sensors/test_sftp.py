from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.sftp.sensors.sftp import SFTPSensorAsync
from astronomer.providers.sftp.triggers.sftp import SFTPTrigger

MODULE = "astronomer.providers.sftp.sensors.sftp"


class TestSFTPSensorAsync:
    @mock.patch(f"{MODULE}.SFTPSensorAsync.poke", return_value=False)
    def test_sftp_run_now_sensor_async(self, context):
        """
        Asserts that a task is deferred and a SFTPTrigger will be fired
        when the SFTPSensorAsync is executed.
        """

        task = SFTPSensorAsync(
            task_id="run_now",
            path="/test/path/",
            file_pattern="test_file",
        )

        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
            assert isinstance(exc.value.trigger, SFTPTrigger), "Trigger is not an SFTPTrigger"

    def test_sftp_execute_complete_success(self, context):
        """
        Asserts that execute_complete doesn't raise an exception if the
        TriggerEvent is marked success
        """
        task = SFTPSensorAsync(task_id="run_now", path="/test/path/", file_pattern="test_file")
        task.execute_complete(context, {"status": "success", "message": "some_file.txt"})

    def test_sftp_execute_complete_failure(self, context):
        """
        Asserts that execute_complete raises an exception if the
        TriggerEvent is marked failure
        """

        task = SFTPSensorAsync(task_id="run_now", path="/test/path/", file_pattern="test_file")
        expected_message = "Some exception message"

        with pytest.raises(AirflowException) as exc:
            task.execute_complete(context, {"status": "error", "message": expected_message})
            assert exc.message == expected_message
