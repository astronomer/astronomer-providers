import pytest
from airflow.exceptions import TaskDeferred

from astronomer.providers.sftp.sensors.sftp import SFTPSensorAsync
from astronomer.providers.sftp.triggers.sftp import SFTPTrigger


def context():
    """Creates an empty context."""
    context = {}
    yield context


def test_sftp_run_now_sensor_async():
    """
    Asserts that a task is deferred and a SFTPTrigger will be fired
    when the SFTPSensorAsync is executed.
    """

    task = SFTPSensorAsync(task_id="run_now", path="/test/path/", file_pattern="test_file")

    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
        assert isinstance(exc.value.trigger, SFTPTrigger), "Trigger is not an SFTPTrigger"
