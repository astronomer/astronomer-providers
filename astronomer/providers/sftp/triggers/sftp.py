import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.sftp.hooks.sftp import SFTPHookAsync


class SFTPTrigger(BaseTrigger):
    """
    Trigger that fires when either the path on the SFTP server does not exist,
    or when there are no files matching the file pattern at the path

    :param path: The path on the SFTP server to search for a file matching the file pattern.
                Authentication method used in the SFTP connection must have access to this path
    :param file_pattern: Pattern to be used for matching against the list of files at the path above.
                Uses the fnmatch module from std library to perform the matching.

    :param sftp_conn_id: SFTP connection ID to be used for connecting to SFTP server
    :param poke_interval: How often, in seconds, to check for the existence of the file on the SFTP server
    """

    def __init__(
        self,
        path: str,
        file_pattern: str = "",
        sftp_conn_id: str = "sftp_default",
        poke_interval: float = 5,
    ) -> None:
        super().__init__()
        self.path = path
        self.file_pattern = file_pattern
        self.sftp_conn_id = sftp_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SFTPTrigger arguments and classpath"""
        return (
            "astronomer.providers.sftp.triggers.sftp.SFTPTrigger",
            {
                "path": self.path,
                "file_pattern": self.file_pattern,
                "sftp_conn_id": self.sftp_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes a series of asynchronous calls to sftp servers via async sftp hook. It yields a Trigger if
        file matching file pattern exists at the specified path, otherwise it throws an exception.
        """
        hook = self._get_async_hook()
        exc = None

        while True:
            try:
                file_returned_by_hook = await hook.get_file_by_pattern(
                    path=self.path, fnmatch_pattern=self.file_pattern
                )
                yield TriggerEvent({"status": "success", "message": f"Sensed file: {file_returned_by_hook}"})
            except AirflowException:
                await asyncio.sleep(self.poke_interval)
            except Exception as e:
                exc = e
                # Break loop to avoid infinite retries on terminal failure
                break

        yield TriggerEvent({"status": "error", "message": exc})

    def _get_async_hook(self) -> SFTPHookAsync:
        return SFTPHookAsync(sftp_conn_id=self.sftp_conn_id)
