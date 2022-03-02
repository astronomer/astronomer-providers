import fnmatch
import logging
import re
from typing import Any

from aiobotocore.session import ClientCreatorContext
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync

log = logging.getLogger(__name__)


class S3HookAsync(AwsBaseHookAsync):
    """
    Interact with AWS S3, using the aiobotocore library.
    """

    conn_type = "s3"
    hook_name = "S3"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "s3"
        kwargs["resource_type"] = "s3"
        super().__init__(*args, **kwargs)

    @staticmethod
    async def _check_exact_key(client: ClientCreatorContext, bucket: str, key: str) -> bool:
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
    async def _check_wildcard_key(client: ClientCreatorContext, bucket: str, wildcard_key: str) -> bool:
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

    async def check_key(
        self, client: ClientCreatorContext, bucket: str, key: str, wildcard_match: bool
    ) -> bool:
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
