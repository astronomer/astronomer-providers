from __future__ import annotations

import asyncio
import warnings
from datetime import datetime
from typing import Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.timezone import convert_to_utc
from dateutil.parser import parse as parse_date

from astronomer.providers.sftp.hooks.sftp import SFTPHookAsync


class SFTPTrigger(BaseTrigger):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.sftp.triggers.sftp.SFTPTrigger` instead.
    """

    def __init__(
        self,
        path: str,
        file_pattern: str = "",
        sftp_conn_id: str = "sftp_default",
        newer_than: datetime | str | None = None,
        poke_interval: float = 5,
    ) -> None:
        warnings.warn(
            "This class is deprecated and will be removed in 2.0.0. "
            "Use `airflow.providers.sftp.triggers.sftp.SFTPTrigger` instead."
        )
        super().__init__()
        self.path = path
        self.file_pattern = file_pattern
        self.sftp_conn_id = sftp_conn_id
        self.newer_than = newer_than
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes SFTPTrigger arguments and classpath"""
        return (
            "astronomer.providers.sftp.triggers.sftp.SFTPTrigger",
            {
                "path": self.path,
                "file_pattern": self.file_pattern,
                "sftp_conn_id": self.sftp_conn_id,
                "newer_than": self.newer_than,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Makes a series of asynchronous calls to sftp servers via async sftp hook. It yields a Trigger

        - If file matching file pattern exists at the specified path return it,
        - If file pattern was not provided, it looks directly into the specific path which was provided.
        - If newer then datetime was provided it looks for the file path last modified time and
          check whether the last modified time is greater, if true return file if false it polls again.
        """
        hook = self._get_async_hook()
        exc = None
        if isinstance(self.newer_than, str):
            self.newer_than = parse_date(self.newer_than)
        _newer_than = convert_to_utc(self.newer_than) if self.newer_than else None
        while True:
            try:
                if self.file_pattern:
                    files_returned_by_hook = await hook.get_files_and_attrs_by_pattern(
                        path=self.path, fnmatch_pattern=self.file_pattern
                    )
                    files_sensed = []
                    for file in files_returned_by_hook:
                        if _newer_than:
                            if file.attrs.mtime is None:
                                continue
                            mod_time = datetime.fromtimestamp(float(file.attrs.mtime)).strftime(
                                "%Y%m%d%H%M%S"
                            )
                            mod_time_utc = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                            if _newer_than <= mod_time_utc:
                                files_sensed.append(file.filename)
                        else:
                            files_sensed.append(file.filename)
                    if files_sensed:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "message": f"Sensed {len(files_sensed)} files: {files_sensed}",
                            }
                        )
                else:
                    mod_time = await hook.get_mod_time(self.path)
                    if _newer_than:
                        mod_time_utc = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                        if _newer_than <= mod_time_utc:
                            yield TriggerEvent({"status": "success", "message": f"Sensed file: {self.path}"})
                    else:
                        yield TriggerEvent({"status": "success", "message": f"Sensed file: {self.path}"})
                await asyncio.sleep(self.poke_interval)
            except AirflowException:
                await asyncio.sleep(self.poke_interval)
            except Exception as e:
                exc = e
                # Break loop to avoid infinite retries on terminal failure
                break

        yield TriggerEvent({"status": "error", "message": exc})

    def _get_async_hook(self) -> SFTPHookAsync:
        return SFTPHookAsync(sftp_conn_id=self.sftp_conn_id)
