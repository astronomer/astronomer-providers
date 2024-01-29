from __future__ import annotations

import warnings
from typing import Any, Callable

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor
from airflow.sensors.base import BaseSensorOperator


class S3KeySensorAsync(S3KeySensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.sensors.s3.S3KeySensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.amazon.aws.sensors.s3.S3KeySensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)


class S3KeySizeSensorAsync(S3KeySensorAsync):
    """
    This class is deprecated.
    Please use :class: `~astronomer.providers.amazon.aws.sensor.s3.S3KeySensorAsync`.
    """

    def __init__(
        self,
        *,
        check_fn: Callable[..., bool] | None = None,
        **kwargs: Any,
    ):
        warnings.warn(
            """
            S3KeySizeSensorAsync is deprecated.
            Please use `astronomer.providers.amazon.aws.sensor.s3.S3KeySensorAsync`.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
        self.check_fn_user = check_fn


class S3KeysUnchangedSensorAsync(S3KeysUnchangedSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.sensors.s3.S3KeysUnchangedSensor`.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.sensors.s3.S3KeysUnchangedSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)


class S3PrefixSensorAsync(BaseSensorOperator):
    """
    This class is deprecated.
    Please use :class: `~astronomer.providers.amazon.aws.sensor.s3.S3KeySensorAsync`.
    """

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str | list[str],
        delimiter: str = "/",
        aws_conn_id: str = "aws_default",
        verify: str | bool | None = None,
        **kwargs: Any,
    ):
        warnings.warn(
            """
            S3PrefixSensor is deprecated.
            Please use `astronomer.providers.amazon.aws.sensor.s3.S3KeySensorAsync`.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = [prefix] if isinstance(prefix, str) else prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.verify = verify
