import logging
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple, Union

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.s3 import S3HookAsync

log = logging.getLogger(__name__)


class S3KeyTrigger(BaseTrigger):
    def __init__(
        self,
        bucket_name: str,
        bucket_key: str,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        **hook_params: Any,
    ):
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serialize S3KeyTrigger arguments and classpath.
        """
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
        """
        Make an asynchronous connection using S3HookAsync.
        """
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
    def __init__(
        self,
        bucket_name: str,
        bucket_key: str,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        check_fn: Optional[Callable[..., bool]] = None,
        **hook_params: Any,
    ):
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params
        self.check_fn_user = check_fn

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serialize S3KeyTrigger arguments and classpath.
        """
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
        return all(f.get('Size', 0) > object_min_size for f in data if isinstance(f, dict))

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Make an asynchronous connection using S3HookAsync.
        """
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
                                return

                        if self._check_fn(s3_objects):
                            yield TriggerEvent({"status": "success"})
                            return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))
