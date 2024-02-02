import warnings
from typing import Any

from airflow.providers.sftp.sensors.sftp import SFTPSensor


class SFTPSensorAsync(SFTPSensor):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.sftp.sensors.sftp.SFTPSensor` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        warnings.warn(
            "This class is deprecated. "
            "Use `airflow.providers.sftp.sensors.sftp.SFTPSensor` instead "
            "and set `deferrable` param to `True` instead."
        )
        super().__init__(*args, deferrable=True, **kwargs)
