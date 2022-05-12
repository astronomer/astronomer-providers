import pytest
from airflow.exceptions import TaskDeferred

from astronomer.providers.core.sensors.filesystem import FileSensorAsync
from astronomer.providers.core.triggers.filesystem import FileTrigger


class TestFileSensorAsync:
    FILE_PATH = "/tmp/sample.py"

    def test_task_defer(self, dag, context):
        """
        Asserts that a task is deferred and an FileTrigger will be fired
        when FileSensorAsync is provided with all required arguments
        """
        task = FileSensorAsync(
            task_id="wait_for_file",
            filepath=self.FILE_PATH,
            dag=dag,
        )

        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)

        assert isinstance(exc.value.trigger, FileTrigger), "Trigger is not a FileTrigger"
