"""This module contains Google Cloud Storage sensors."""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)


class GCSObjectExistenceSensorAsync(GCSObjectExistenceSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            kwargs["poke_interval"] = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)


class GCSObjectsWithPrefixExistenceSensorAsync(GCSObjectsWithPrefixExistenceSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            kwargs["poke_interval"] = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        # https://github.com/python/mypy/issues/6799#issuecomment-1882059741
        super().__init__(*args, deferrable=True, **kwargs)  # type: ignore[misc]


class GCSUploadSessionCompleteSensorAsync(GCSUploadSessionCompleteSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            kwargs["poke_interval"] = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        # https://github.com/python/mypy/issues/6799#issuecomment-1882059741
        super().__init__(*args, deferrable=True, **kwargs)  # type: ignore[misc]


class GCSObjectUpdateSensorAsync(GCSObjectUpdateSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
        polling_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            kwargs["poke_interval"] = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        # https://github.com/python/mypy/issues/6799#issuecomment-1882059741
        super().__init__(*args, deferrable=True, **kwargs)  # type: ignore[misc]
