import asyncio
import datetime
import logging
import os
from glob import glob
from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)


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
            "astronomer_operators.core.triggers.filesystem.FileTrigger",
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
                    mod_time = datetime.datetime.fromtimestamp(mod_time).strftime("%Y%m%d%H%M%S")
                    log.info("Found File %s last modified: %s", str(path), str(mod_time))
                    yield TriggerEvent(True)
                    return
                for _, _, files in os.walk(self.filepath):
                    if len(files) > 0:
                        yield TriggerEvent(True)
                        return
            await asyncio.sleep(self.poll_interval)
