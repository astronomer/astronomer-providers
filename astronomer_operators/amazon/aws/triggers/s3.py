import logging
from typing import Any, Dict, Optional, Tuple, Union

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.amazon.aws.hooks.s3 import S3HookAsync

log = logging.getLogger(__name__)


class S3Trigger(BaseTrigger):
    def __init__(
        self,
        bucket_name: str,
        bucket_key: str,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        verify: Optional[Union[str, bool]] = None,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serialize S3Trigger arguments and classpath.
        """
        return (
            "astronomer_operators.amazon.aws.triggers.s3.S3Trigger",
            {
                "bucket_name": self.bucket_name,
                "bucket_key": self.bucket_key,
                "wildcard_match": self.wildcard_match,
                "aws_conn_id": self.aws_conn_id,
                "verify": self.verify,
            },
        )

    async def run(self):
        """
        Make an asynchronous connection using S3HookAsync.
        """
        hook = self._get_async_hook()
        while True:
            self.log.info("Poking for key : s3://%s/%s", self.bucket_name, self.bucket_key)
            if self.wildcard_match:
                response_for_wildcard = await hook.check_for_wildcard_key(self.bucket_key, self.bucket_name)
                if response_for_wildcard:
                    yield TriggerEvent(True)
                    return
            response_for_key = await hook.check_for_key(self.bucket_key, self.bucket_name)
            if response_for_key:
                yield TriggerEvent(True)
                return

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(
            aws_conn_id=self.aws_conn_id, verify=self.verify, client_type="s3", resource_type="s3"
        )
