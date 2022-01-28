import fnmatch
import logging
import re
from typing import Any, Dict, Optional, Tuple, Union

from airflow.triggers.base import BaseTrigger, TriggerEvent
from botocore.exceptions import ClientError

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

    @staticmethod
    async def _check_exact_key(client, bucket, key) -> bool:
        """
        Checks if a key exists in a bucket asynchronously
        :param client: aiobotocore client
        :type client: aiobotocore.client.S3
        :param bucket: Name of the bucket in which the file is stored
        :type bucket: str
        :param key: S3 key that will point to the file
        :type key: str
        :return: True if the key exists and False if not.
        :rtype: bool
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
    async def _check_wildcard_key(client, bucket, wildcard_key):
        """
        Checks that a key matching a wildcard expression exists in a bucket asynchronously
        :param client: aiobotocore client
        :type client: aiobotocore.client.S3
        :param bucket: the name of the bucket
        :type bucket: str
        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :return: True if a key exists and False if not.
        :rtype: bool
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

    async def _check_key(self, client, bucket, key, wildcard) -> bool:
        """
        Checks if key exists or a key matching a wildcard expression exists in a bucket asynchronously
        :param client: aiobotocore client
        :type client: aiobotocore.client.S3
        :param bucket: the name of the bucket
        :type bucket: str
        :param key: S3 key that will point to the file
        :type key: str
        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :return: True if a key exists and False if not.
        :rtype: bool
        """
        if wildcard:
            return await self._check_wildcard_key(client, bucket, key)
        else:
            return await self._check_exact_key(client, bucket, key)

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
        async with await hook.get_client_async() as client:
            while True:
                self.log.info("Poking for key : s3://%s/%s", self.bucket_name, self.bucket_key)
                if await self._check_key(client, self.bucket_name, self.bucket_key, self.wildcard_match):
                    yield TriggerEvent(True)

    def _get_async_hook(self) -> S3HookAsync:
        return S3HookAsync(aws_conn_id=self.aws_conn_id, verify=self.verify)
