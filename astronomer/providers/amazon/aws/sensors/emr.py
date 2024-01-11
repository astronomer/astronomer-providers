from __future__ import annotations

import warnings

from airflow.providers.amazon.aws.sensors.emr import (
    EmrContainerSensor,
    EmrJobFlowSensor,
    EmrStepSensor,
)


class EmrContainerSensorAsync(EmrContainerSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor`.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        poll_interval = kwargs.pop("poll_interval")
        if poll_interval:
            self.poke_interval = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)


class EmrStepSensorAsync(EmrStepSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.sensors.emr.EmrStepSensor`.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.sensors.emr.EmrStepSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)


class EmrJobFlowSensorAsync(EmrJobFlowSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.sensors.emr.EmrJobFlowSensor`.
    """

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.sensors.emr.EmrJobFlowSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)
