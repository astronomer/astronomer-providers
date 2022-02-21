import logging
import os

from airflow.hooks.filesystem import FSHook
from airflow.sensors.filesystem import FileSensor

from astronomer.providers.core.triggers.filesystem import FileTrigger

log = logging.getLogger(__name__)


class FileSensorAsync(FileSensor):
    """
    Waits for a file or folder to land in a filesystem using async.

    If the path given is a directory then this sensor will only return true if
    any files exist inside it (either directly, or within a subdirectory)

    :param fs_conn_id: reference to the File (path)
    :type fs_conn_id: str
    :param filepath: File or folder name (relative to the base path set within the connection), can
        be a glob.
    :type filepath: str
    :param recursive: when set to ``True``, enables recursive directory matching behavior of
        ``**`` in glob filepath parameter. Defaults to ``False``.
    :type recursive: bool
    """

    def execute(self, context):
        if not self.poke(context=context):
            hook = FSHook(self.fs_conn_id)
            basepath = hook.get_path()
            full_path = os.path.join(basepath, self.filepath)
            self.log.info("Poking for file %s", full_path)

            self.defer(
                timeout=self.execution_timeout,
                trigger=FileTrigger(
                    filepath=full_path,
                    recursive=self.recursive,
                    poll_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None
