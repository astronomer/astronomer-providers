import warnings
from typing import Any

from airflow.providers.sftp.sensors.sftp import SFTPSensor


class SFTPSensorAsync(SFTPSensor):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.sftp.sensors.sftp.SFTPSensor` instead
    and set `deferrable` param to `True` instead.
    """

    is_deprecated = True
    post_deprecation_replacement = "from airflow.providers.sftp.sensors.sftp import SFTPSensor"

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
