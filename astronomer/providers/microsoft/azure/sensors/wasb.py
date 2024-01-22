import warnings
from typing import Any, List, Optional

from airflow.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensor,
    WasbPrefixSensor,
)


class WasbBlobSensorAsync(WasbBlobSensor):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.microsoft.azure.sensors.wasb.WasbBlobSensor` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        poll_interval: float = 5.0,
        **kwargs: Any,
    ):
        warnings.warn(
            (
                "This class is deprecated. "
                "Use `airflow.providers.microsoft.azure.sensors.wasb.WasbBlobSensor` "
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        # TODO: Remove once deprecated
        if poll_interval:
            kwargs["poke_interval"] = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(*args, deferrable=True, **kwargs)


class WasbPrefixSensorAsync(WasbPrefixSensor):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.microsoft.azure.sensors.wasb.WasbPrefixSensor` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        include: Optional[List[str]] = None,
        delimiter: Optional[str] = "/",
        poll_interval: float = 5.0,
        **kwargs: Any,
    ):
        warnings.warn(
            (
                "This class is deprecated. "
                "Use `airflow.providers.microsoft.azure.sensors.wasb.WasbPrefixSensor` "
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        # TODO: Remove once deprecated
        if poll_interval:
            self.poke_interval = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        if kwargs.get("check_options") is None:
            kwargs["check_options"] = {}
        kwargs["check_options"]["include"] = include
        kwargs["check_options"]["delimiter"] = delimiter
        super().__init__(*args, deferrable=True, **kwargs)
