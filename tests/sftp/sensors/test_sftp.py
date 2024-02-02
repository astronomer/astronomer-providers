from airflow.providers.sftp.sensors.sftp import SFTPSensor

from astronomer.providers.sftp.sensors.sftp import SFTPSensorAsync


class TestSFTPSensorAsync:
    def test_init(self):
        task = SFTPSensorAsync(
            task_id="run_now",
            path="/test/path/",
            file_pattern="test_file",
        )

        assert isinstance(task, SFTPSensor)
        assert task.deferrable is True
