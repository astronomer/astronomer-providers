"""This module contains Google Big Query sensors."""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor


class BigQueryTableExistenceSensorAsync(BigQueryTableExistenceSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.sensors.bigquery.BigQueryTableExistenceSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
        # TODO: Remove once deprecated
        if polling_interval:
            self.poke_interval = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.gcp_conn_id = gcp_conn_id
