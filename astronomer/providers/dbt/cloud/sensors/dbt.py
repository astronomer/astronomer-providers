from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor


class DbtCloudJobRunSensorAsync(DbtCloudJobRunSensor):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunSensor` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated. "
                "Use `airflow.providers.dbt.cloud.sensors.dbt.DbtCloudJobRunSensor` "
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        # TODO: Remove once deprecated
        if kwargs.get("poll_interval"):
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs["poke_interval"] = kwargs.pop("poll_interval")
        super().__init__(*args, deferrable=True, **kwargs)
