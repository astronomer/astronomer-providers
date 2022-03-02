import logging
from typing import Any, AsyncIterator, Dict, Tuple

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
        hook = self._get_async_hook()
        async with await hook.get_client_async() as client:
            while True:
                self.log.info("Poking for key : s3://%s/%s", self.bucket_name, self.bucket_key)
                if await hook.check_key(client, self.bucket_name, self.bucket_key, self.wildcard_match):
                    yield TriggerEvent(True)

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))
