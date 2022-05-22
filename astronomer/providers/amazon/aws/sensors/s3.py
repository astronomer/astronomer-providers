import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Union, cast
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import Context

from astronomer.providers.amazon.aws.triggers.s3 import (
    S3KeySizeTrigger,
    S3KeysUnchangedTrigger,
    S3KeyTrigger,
    S3PrefixTrigger,
)

log = logging.getLogger(__name__)


class S3KeySensorAsync(BaseOperator):
    """
    Waits for a key (a file-like instance on S3) to be present in a S3 bucket
    asynchronously. S3 being a key/value it does not support folders. The path
    is just a key a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
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
    """

    template_fields: Sequence[str] = ("bucket_key", "bucket_name")

    def __init__(
        self,
        *,
        bucket_key: str,
        bucket_name: Optional[str] = None,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        verify: Optional[Union[str, bool]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: Optional[S3Hook] = None

    def _resolve_bucket_and_key(self) -> None:
        """If key is URI, parse bucket"""
        if self.bucket_name is None:
            self.bucket_name, self.bucket_key = S3Hook.parse_s3_url(self.bucket_key)
        else:
            parsed_url = urlparse(self.bucket_key)
            if parsed_url.scheme != "" or parsed_url.netloc != "":
                raise AirflowException("If bucket_name provided, bucket_key must be relative path, not URI.")

    def execute(self, context: Context) -> None:
        """Check for a key in s3 and defers using the trigger"""
        self._resolve_bucket_and_key()
        self.defer(
            timeout=self.execution_timeout,
            trigger=S3KeyTrigger(
                bucket_name=cast(str, self.bucket_name),
                bucket_key=self.bucket_key,
                wildcard_match=self.wildcard_match,
                aws_conn_id=self.aws_conn_id,
                verify=self.verify,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        return None


class S3KeySizeSensorAsync(S3KeySensorAsync):
    """
    Waits for a key (a file-like instance on S3) to be present and be more than
    some size in a S3 bucket asynchronously.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
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
    :param check_fn: Function that receives the list of the S3 objects,
        and returns the boolean:
        - ``True``: a certain criteria is met
        - ``False``: the criteria isn't met
        **Example**: Wait for any S3 object size more than 1 megabyte  ::

            def check_fn(self, data: List) -> bool:
                return any(f.get('Size', 0) > 1048576 for f in data if isinstance(f, dict))
    """

    def __init__(
        self,
        *,
        check_fn: Optional[Callable[..., bool]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.check_fn_user = check_fn

    def execute(self, context: Context) -> None:
        """Defers using the trigger, and check for the file size"""
        self._resolve_bucket_and_key()
        self.defer(
            timeout=self.execution_timeout,
            trigger=S3KeySizeTrigger(
                bucket_name=cast(str, self.bucket_name),
                bucket_key=self.bucket_key,
                wildcard_match=self.wildcard_match,
                aws_conn_id=self.aws_conn_id,
                verify=self.verify,
                check_fn_user=self.check_fn_user,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        return None


class S3KeysUnchangedSensorAsync(BaseOperator):
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

    template_fields: Sequence[str] = ("bucket_name", "prefix")

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str,
        aws_conn_id: str = "aws_default",
        verify: Optional[Union[bool, str]] = None,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: Optional[Set[str]] = None,
        allow_delete: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects or set()
        self.inactivity_seconds = 0
        self.allow_delete = allow_delete
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.last_activity_time: Optional[datetime] = None

    def execute(self, context: Context) -> None:
        """Defers Trigger class to check for changes in the number of objects at prefix in AWS S3"""
        self.defer(
            timeout=self.execution_timeout,
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

    def execute_complete(self, context: Dict[str, Any], event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        return None


class S3PrefixSensorAsync(BaseOperator):
    """
    Async implementation of the S3 Prefix Sensor.
    Gets deferred onto the Trigggerer and pokes
    for a prefix or all prefixes to exist.
    A prefix is the first part of a key,thus enabling
    checking of constructs similar to glob ``airfl*`` or
    SQL LIKE ``'airfl%'``. There is the possibility to precise a delimiter to
    indicate the hierarchy or keys, meaning that the match will stop at that
    delimiter. Current code accepts sane delimiters, i.e. characters that
    are NOT special characters in the Python regex engine.

    :param bucket_name: Name of the S3 bucket
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :param delimiter: The delimiter intended to show hierarchy.
        Defaults to '/'.
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether to verify SSL certificates for S3 connection.
        By default, SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    """

    template_fields: Sequence[str] = ("prefix", "bucket_name")

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
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.prefix = [prefix] if isinstance(prefix, str) else prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context: Context) -> None:
        """Defers trigger class to poke for a prefix or all prefixes to exist"""
        self.log.info("Poking for prefix : %s in bucket s3://%s", self.prefix, self.bucket_name)
        self.defer(
            timeout=self.execution_timeout,
            trigger=S3PrefixTrigger(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                delimiter=self.delimiter,
                aws_conn_id=self.aws_conn_id,
                verify=self.verify,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[Any, Any], event: Dict[str, str]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])
        return None
