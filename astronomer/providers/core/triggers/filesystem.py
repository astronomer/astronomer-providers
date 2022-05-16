import asyncio
import datetime
import os
import typing
from glob import glob
from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent


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
        """Serializes FileTrigger arguments and classpath."""
        return (
            "astronomer.providers.core.triggers.filesystem.FileTrigger",
            {
                "filepath": self.filepath,
                "recursive": self.recursive,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> typing.AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the relevant files are found."""
        while True:
            for path in glob(self.filepath, recursive=self.recursive):
                if os.path.isfile(path):
                    mod_time_f = os.path.getmtime(path)
                    mod_time = datetime.datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                    self.log.info("Found File %s last modified: %s", str(path), str(mod_time))
                    yield TriggerEvent(True)
                for _, _, files in os.walk(self.filepath):
                    if len(files) > 0:
                        yield TriggerEvent(True)
            await asyncio.sleep(self.poll_interval)
