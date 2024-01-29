import warnings
from typing import Any

from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor


class RedshiftClusterSensorAsync(RedshiftClusterSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.sensors.redshift_cluster.RedshiftClusterSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        **kwargs: Any,
    ):
        # TODO: Remove once deprecated
        if kwargs.get("poll_interval"):
            kwargs["poke_interval"] = kwargs["poll_interval"]
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.amazon.aws.sensors.redshift_cluster.RedshiftClusterSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
