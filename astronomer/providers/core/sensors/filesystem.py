from __future__ import annotations

import os
import warnings
from datetime import timedelta
from typing import Any

from airflow.hooks.filesystem import FSHook
from airflow.sensors.filesystem import FileSensor

from astronomer.providers.core.triggers.filesystem import FileTrigger
from astronomer.providers.utils.typing_compat import Context


class FileSensorAsync(FileSensor):
    """
    Waits for a file or folder to land in a filesystem using async.

    If the path given is a directory then this sensor will only return true if
    any files exist inside it (either directly, or within a subdirectory)

    :param fs_conn_id: reference to the File (path)
    :param filepath: File or folder name (relative to the base path set within the connection), can
        be a glob.
    :param recursive: when set to ``True``, enables recursive directory matching behavior of
        ``**`` in glob filepath parameter. Defaults to ``False``.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This module is deprecated and will be removed in airflow>=2.9.0"
                "Please use `airflow.sensors.filesystem.FileSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        if not self.poke(context=context):
            hook = FSHook(self.fs_conn_id)
            basepath = hook.get_path()
            full_path = os.path.join(basepath, self.filepath)
            self.log.info("Poking for file %s", full_path)

            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=FileTrigger(
                    filepath=full_path,
                    recursive=self.recursive,
                    poll_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: bool | None = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
