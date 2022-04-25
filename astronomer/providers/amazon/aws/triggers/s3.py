import asyncio
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set, Tuple, Union

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.s3 import S3HookAsync

log = logging.getLogger(__name__)


class S3KeyTrigger(BaseTrigger):
    """
    S3KeyTrigger is fired as deferred class with params to run the task in trigger worker

    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param bucket_key:  The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param aws_conn_id: reference to the s3 connection
    :param hook_params: params for hook its optional
    """

    def __init__(
        self,
        bucket_name: str,
        bucket_key: str,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        **hook_params: Any,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize S3KeyTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.s3.S3KeyTrigger",
            {
                "bucket_name": self.bucket_name,
                "bucket_key": self.bucket_key,
                "wildcard_match": self.wildcard_match,
                "aws_conn_id": self.aws_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make an asynchronous connection using S3HookAsync."""
        try:
            hook = self._get_async_hook()
            async with await hook.get_client_async() as client:
                while True:
                    self.log.info("Poking for key : s3://%s/%s", self.bucket_name, self.bucket_key)
                    if await hook.check_key(client, self.bucket_name, self.bucket_key, self.wildcard_match):
                        yield TriggerEvent({"status": "success"})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))


class S3KeySizeTrigger(BaseTrigger):
    """
    S3KeySizeTrigger is fired as deferred class with params to run the task in trigger worker,
    S3 Objects have size more than 0

    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param bucket_key:  The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param aws_conn_id: reference to the s3 connection
    :param check_fn: Function that receives the list of the S3 objects,
        and returns the boolean
    :param hook_params: params for hook its optional
    """

    def __init__(
        self,
        bucket_name: str,
        bucket_key: str,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        check_fn: Optional[Callable[..., bool]] = None,
        **hook_params: Any,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params
        self.check_fn_user = check_fn

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize S3KeySizeTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.s3.S3KeySizeTrigger",
            {
                "bucket_name": self.bucket_name,
                "bucket_key": self.bucket_key,
                "wildcard_match": self.wildcard_match,
                "aws_conn_id": self.aws_conn_id,
                "hook_params": self.hook_params,
                "check_fn_user": self.check_fn_user,
            },
        )

    @staticmethod
    def _check_fn(data: List[Any], object_min_size: Optional[Union[int, float]] = 0) -> bool:
        """Default function for checking that S3 Objects have size more than 0
        :param data: List of the objects in S3 bucket.
        :param object_min_size: Checks if the objects sizes are greater then this value.
        """
        return all(f.get("Size", 0) > object_min_size for f in data if isinstance(f, dict))

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make an asynchronous connection using S3HookAsync."""
        try:
            hook = self._get_async_hook()
            async with await hook.get_client_async() as client:
                while True:
                    self.log.info("Poking for key : s3://%s/%s", self.bucket_name, self.bucket_key)
                    if await hook.check_key(client, self.bucket_name, self.bucket_key, self.wildcard_match):
                        s3_objects = await hook.get_files(
                            client, self.bucket_name, self.bucket_key, self.wildcard_match
                        )

                        if self.check_fn_user is not None:
                            if self.check_fn_user(s3_objects):
                                yield TriggerEvent({"status": "success"})

                        if self._check_fn(s3_objects):
                            yield TriggerEvent({"status": "success"})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))


class S3KeysUnchangedTrigger(BaseTrigger):
    """
    S3KeyTrigger is fired as deferred class with params to run the task in trigger worker

    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :param inactivity_period: The total seconds of inactivity to designate
        keys unchanged. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for keys unchanged
        sensor to be considered valid.
    :param inactivity_seconds: reference to the seconds of inactivity
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
    :param aws_conn_id: reference to the s3 connection
    :param last_activity_time: last modified or last active time
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        inactivity_seconds: int = 0,
        previous_objects: Optional[Set[str]] = None,
        allow_delete: bool = True,
        aws_conn_id: str = "aws_default",
        last_activity_time: Optional[datetime] = None,
        verify: Optional[Union[bool, str]] = None,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        if previous_objects is None:
            previous_objects = set()
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects
        self.inactivity_seconds = inactivity_seconds
        self.allow_delete = allow_delete
        self.aws_conn_id = aws_conn_id
        self.last_activity_time: Optional[datetime] = last_activity_time
        self.verify = verify
        self.polling_period_seconds = 0

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize S3KeysUnchangedTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.s3.S3KeysUnchangedTrigger",
            {
                "bucket_name": self.bucket_name,
                "prefix": self.prefix,
                "inactivity_period": self.inactivity_period,
                "min_objects": self.min_objects,
                "previous_objects": self.previous_objects,
                "inactivity_seconds": self.inactivity_seconds,
                "allow_delete": self.allow_delete,
                "aws_conn_id": self.aws_conn_id,
                "last_activity_time": self.last_activity_time,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make an asynchronous connection using S3HookAsync."""
        try:
            hook = self._get_async_hook()
            async with await hook.get_client_async() as client:
                while True:
                    result = await hook.is_keys_unchanged(
                        client,
                        self.bucket_name,
                        self.prefix,
                        self.inactivity_period,
                        self.min_objects,
                        self.previous_objects,
                        self.inactivity_seconds,
                        self.allow_delete,
                        self.last_activity_time,
                    )
                    if result.get("status") == "success" or result.get("status") == "error":
                        yield TriggerEvent(result)
                    elif result.get("status") == "pending":
                        self.previous_objects = result.get("previous_objects", set())
                        self.last_activity_time = result.get("last_activity_time")
                        self.inactivity_seconds = result.get("inactivity_seconds", 0)
                    await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.verify)


class S3PrefixTrigger(BaseTrigger):
    """
    S3PrefixTrigger class is fired as deferred class with params to run the task in trigger worker

    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :param delimiter: The delimiter intended to show hierarchy.
    :param aws_conn_id: reference to the s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
    :param hook_params: params for hook its optional
    """

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: Union[str, List[str]],
        delimiter: str = "/",
        aws_conn_id: str = "aws_default",
        verify: Optional[Union[str, bool]] = None,
        **hook_params: Any,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.prefix = [prefix] if isinstance(prefix, str) else prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize S3PrefixTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.s3.S3PrefixTrigger",
            {
                "bucket_name": self.bucket_name,
                "prefix": self.prefix,
                "delimiter": self.delimiter,
                "aws_conn_id": self.aws_conn_id,
                "verify": self.verify,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make an asynchronous connection using S3HookAsync."""
        hook = self._get_async_hook()
        try:
            async with await hook.get_client_async() as client:
                while True:
                    self.log.info("Poking for prefix : %s in bucket s3://%s", self.prefix, self.bucket_name)
                    buffer = []
                    for elem in (
                        hook._check_for_prefix(client, prefix, self.delimiter, self.bucket_name)
                        for prefix in self.prefix
                    ):
                        buffer.append(await elem)
                    if all(buffer):
                        yield TriggerEvent({"status": "success", "message": "Success criteria met. Exiting."})

        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))
