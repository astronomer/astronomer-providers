import fnmatch
import logging
import re
from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.s3 import S3HookAsync

log = logging.getLogger(__name__)


class S3KeyTrigger(BaseTrigger):
    def __init__(
        self,
        bucket_name: str,
        bucket_key: str,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        **hook_params,
    ):
        super().__init__()
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.hook_params = hook_params

    @staticmethod
    async def _check_exact_key(client, bucket, key) -> bool:
        """
        Checks if a key exists in a bucket asynchronously

        :param client: aiobotocore client
        :param bucket: Name of the bucket in which the file is stored
        :param key: S3 key that will point to the file
        :return: True if the key exists and False if not.
        """
        try:
            await client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            else:
                raise e

    @staticmethod
    async def _check_wildcard_key(client, bucket: str, wildcard_key: str) -> bool:
        """
        Checks that a key matching a wildcard expression exists in a bucket asynchronously

        :param client: aiobotocore client
        :param bucket: the name of the bucket
        :param wildcard_key: the path to the key
        :return: True if a key exists and False if not.
        """
        prefix = re.split(r"[\[\*\?]", wildcard_key, 1)[0]
        delimiter = ""
        paginator = client.get_paginator("list_objects_v2")
        response = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
        async for page in response:
            if "Contents" in page:
                for k in page["Contents"]:
                    if fnmatch.fnmatch(k["Key"], wildcard_key):
                        return True
        return False

    async def _check_key(self, client, bucket: str, key: str, wildcard_match: bool) -> bool:
        """
        Checks if key exists or a key matching a wildcard expression exists in a bucket asynchronously

        :param client: aiobotocore client
        :param bucket: the name of the bucket
        :param key: S3 key that will point to the file
        :param wildcard_match: the path to the key
        :return: True if a key exists and False if not.
        """
        if wildcard_match:
            return await self._check_wildcard_key(client, bucket, key)
        else:
            return await self._check_exact_key(client, bucket, key)

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

    async def run(self):
        """
        Make an asynchronous connection using S3HookAsync.
        """
        hook = self._get_async_hook()
        async with await hook.get_client_async() as client:
            while True:
                self.log.info("Poking for key : s3://%s/%s", self.bucket_name, self.bucket_key)
                if await self._check_key(client, self.bucket_name, self.bucket_key, self.wildcard_match):
                    yield TriggerEvent(True)

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.hook_params.get("verify"))
