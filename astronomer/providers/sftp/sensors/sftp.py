from datetime import timedelta
from typing import Any, Dict, Optional

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.sftp.sensors.sftp import SFTPSensor

from astronomer.providers.sftp.hooks.sftp import SFTPHookAsync
from astronomer.providers.sftp.triggers.sftp import SFTPTrigger
from astronomer.providers.utils.typing_compat import Context


class SFTPSensorAsync(SFTPSensor):
    """
    Polls an SFTP server continuously until a file_pattern is matched at a defined path

    :param path: The path on the SFTP server to search for a file matching the file pattern.
                 Authentication method used in the SFTP connection must have access to this path
    :param file_pattern: Pattern to be used for matching against the list of files at the path above.
                 Uses the fnmatch module from std library to perform the matching.
    :param timeout: How long, in seconds, the sensor waits for successful before timing out
    :param newer_than: DateTime for which the file or file path should be newer than, comparison is inclusive
    """

    def __init__(
        self,
        *,
        path: str,
        file_pattern: str = "",
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> None:

        self.path = path
        self.file_pattern = file_pattern
        if timeout is None:
            timeout = conf.getfloat("sensors", "default_timeout")

        super().__init__(path=path, file_pattern=file_pattern, timeout=timeout, **kwargs)
        self.hook = SFTPHookAsync(sftp_conn_id=self.sftp_conn_id)  # type: ignore[assignment]

    def execute(self, context: Context) -> None:
        """
        Logic that the sensor uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=SFTPTrigger(
                path=self.path,
                file_pattern=self.file_pattern,
                sftp_conn_id=self.sftp_conn_id,
                poke_interval=self.poke_interval,
                newer_than=self.newer_than,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event is not None:
            if "status" in event and event["status"] == "error":
                raise AirflowException(event["message"])

            if "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info(event["message"])
                return None

        raise AirflowException("No event received in trigger callback")
