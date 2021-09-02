import asyncio
import datetime
import logging
import os
from glob import glob
from typing import Any, Dict, Tuple

from airflow.hooks.filesystem import FSHook
from airflow.sensors.filesystem import FileSensor
from airflow.triggers.base import BaseTrigger, TriggerEvent

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


class FileTrigger(BaseTrigger):
    """
    A trigger that fires exactly once after it finds the requested file or folder.

    :param filepath: File or folder name (relative to the base path set within the connection), can
        be a glob.
    :type filepath: str
    :param recursive: when set to ``True``, enables recursive directory matching behavior of
        ``**`` in glob filepath parameter. Defaults to ``False``.
    :type recursive: bool
    """

    def __init__(
        self,
        filepath: str,
        recursive: bool = False,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.filepath = filepath
        self.recursive = recursive
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes FileTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.filesystem.FileTrigger",
            {
                "filepath": self.filepath,
                "recursive": self.recursive,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Simple loop until the relevant files are found.
        """
        while True:
            for path in glob(self.filepath, recursive=self.recursive):
                if os.path.isfile(path):
                    mod_time = os.path.getmtime(path)
                    mod_time = datetime.datetime.fromtimestamp(mod_time).strftime(
                        "%Y%m%d%H%M%S"
                    )
                    log.info(
                        "Found File %s last modified: %s", str(path), str(mod_time)
                    )
                    yield TriggerEvent(True)
                    return
                for _, _, files in os.walk(self.filepath):
                    if len(files) > 0:
                        yield TriggerEvent(True)
                        return
            await asyncio.sleep(self.poll_interval)
