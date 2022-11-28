import typing
import warnings
from datetime import timedelta
from typing import Any, Callable, List, Optional, Sequence, Union, cast

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensor
from airflow.sensors.base import BaseSensorOperator

from astronomer.providers.amazon.aws.triggers.s3 import (
    S3KeysUnchangedTrigger,
    S3KeyTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class S3KeySensorAsync(BaseSensorOperator):
    """
    Waits for one or multiple keys (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key(s) being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url. When specified, all the keys passed to ``bucket_key``
        refers to this bucket
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param check_fn: Function that receives the list of the S3 objects,
        and returns a boolean:
        - ``True``: the criteria is met
        - ``False``: the criteria isn't met
        **Example**: Wait for any S3 object size more than 1 megabyte  ::

            def check_fn(files: List) -> bool:
                return any(f.get('Size', 0) > 1048576 for f in files)
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.

    .. seealso::
        `For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:S3KeySensor`.
    """

    template_fields: Sequence[str] = ("bucket_key", "bucket_name")

    def __init__(
        self,
        *,
        bucket_key: Union[str, List[str]],
        bucket_name: Optional[str] = None,
        wildcard_match: bool = False,
        check_fn: Optional[Callable[..., bool]] = None,
        aws_conn_id: str = "aws_default",
        verify: Optional[Union[str, bool]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = [bucket_key] if isinstance(bucket_key, str) else bucket_key
        self.wildcard_match = wildcard_match
        self.check_fn = check_fn
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: Optional[S3Hook] = None

    def execute(self, context: Context) -> None:
        """Check for a keys in s3 and defers using the trigger"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=S3KeyTrigger(
                bucket_name=cast(str, self.bucket_name),
                bucket_key=self.bucket_key,
                wildcard_match=self.wildcard_match,
                check_fn=self.check_fn,
                aws_conn_id=self.aws_conn_id,
                verify=self.verify,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Any = None) -> Optional[bool]:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        elif event["status"] == "success" and "s3_objects" in event:
            files = typing.cast(List[str], event["s3_objects"])
            if self.check_fn:
                return self.check_fn(files)
        return None


class S3KeySizeSensorAsync(S3KeySensorAsync):
    """
    This class is deprecated.
    Please use :class: `~astronomer.providers.amazon.aws.sensor.s3.S3KeySensorAsync`.
    """

    def __init__(
        self,
        *,
        check_fn: Optional[Callable[..., bool]] = None,
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
    Checks for changes in the number of objects at prefix in AWS S3
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will not behave correctly
    in reschedule mode, as the state of the listed objects in the S3 bucket will
    be lost between rescheduled invocations.

    :param bucket_name: Name of the S3 bucket
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param inactivity_period: The total seconds of inactivity to designate
        keys unchanged. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for keys unchanged
        sensor to be considered valid.
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    """

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Defers Trigger class to check for changes in the number of objects at prefix in AWS S3"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=S3KeysUnchangedTrigger(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                inactivity_period=self.inactivity_period,
                min_objects=self.min_objects,
                previous_objects=self.previous_objects,
                inactivity_seconds=self.inactivity_seconds,
                allow_delete=self.allow_delete,
                aws_conn_id=self.aws_conn_id,
                verify=self.verify,
                last_activity_time=self.last_activity_time,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        return None


class S3PrefixSensorAsync(BaseSensorOperator):
    """
    This class is deprecated.
    Please use :class: `~astronomer.providers.amazon.aws.sensor.s3.S3KeySensorAsync`.
    """

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: Union[str, List[str]],
        delimiter: str = "/",
        aws_conn_id: str = "aws_default",
        verify: Optional[Union[str, bool]] = None,
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
