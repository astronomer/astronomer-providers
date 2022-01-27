import fnmatch
import logging
import re
from typing import Optional

import aiobotocore.response
from botocore.exceptions import ClientError

from astronomer_operators.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync

log = logging.getLogger(__name__)


class S3HookAsync(AwsBaseHookAsync):
    """
    Interact with AWS S3, using the aiobotocore library.
    """

    conn_type = "s3"
    hook_name = "S3"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "s3"
        super().__init__(*args, **kwargs)

    async def check_for_key(self, key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Checks if a key exists in a bucket asynchronously
        :param key: S3 key that will point to the file
        :type key: str
        :param bucket_name: Name of the bucket in which the file is stored
        :type bucket_name: str
        :return: True if the key exists and False if not.
        :rtype: bool
        """

        try:
            async with self.get_client_async() as client:
                await client.head_object(Bucket=bucket_name, Key=key)
                return True
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            else:
                raise e

    async def check_for_wildcard_key(
        self, wildcard_key: str, bucket_name: Optional[str] = None, delimiter: str = ""
    ) -> bool:
        """
        Checks that a key matching a wildcard expression exists in a bucket asynchronously
        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param delimiter: the delimiter marks key hierarchy
        :type delimiter: str
        :return: True if a key exists and False if not.
        :rtype: bool
        """
        response = await self.get_wildcard_key(
            wildcard_key=wildcard_key, bucket_name=bucket_name, delimiter=delimiter
        )
        return response is not None

    async def get_wildcard_key(
        self, wildcard_key: str, bucket_name: Optional[str] = None, delimiter: str = ""
    ) -> aiobotocore.response.StreamingBody:
        """
        Returns a aiobotocore.response.StreamingBody matching the wildcard expression asynchronously
        :param wildcard_key: the path to the key
        :param bucket_name: the name of the bucket
        :param delimiter: the delimiter marks key hierarchy
        :return: the key object from the bucket or None if none has been found.
        :rtype: aiobotocore.response.StreamingBody
        """
        prefix = re.split(r"[\[\*\?]", wildcard_key, 1)[0]
        key_list = await self.list_keys(bucket_name, prefix=prefix, delimiter=delimiter)
        key_matches = [k for k in key_list if fnmatch.fnmatch(k, wildcard_key)]
        if key_matches:
            response = await self.get_key(key_matches[0], bucket_name)
            return response
        return None

    async def get_key(
        self, key: str, bucket_name: Optional[str] = None
    ) -> aiobotocore.response.StreamingBody:
        """
        Returns a aiobotocore.response.StreamingBody
        :param key: the path to the key
        :param bucket_name: the name of the bucket
        :return: the key object from the bucket
        :rtype: aiobotocore.response.StreamingBody
        """
        async with self.get_client_async() as client:
            response = await client.get_object(Bucket=bucket_name, Key=key)
            return response.get("Body")

    async def list_keys(
        self,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> list:
        """
        Lists keys in a bucket under prefix and not containing delimiter
        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: a list of matched keys
        :rtype: list
        """
        prefix = prefix or ""
        delimiter = delimiter or ""
        config = {
            "PageSize": page_size,
            "MaxItems": max_items,
        }
        async with self.get_client_async() as client:
            paginator = client.get_paginator("list_objects_v2")
            response = paginator.paginate(
                Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config
            )

            response = await response.build_full_result()
            keys = []
            if "Contents" in response:
                for k in response["Contents"]:
                    keys.append(k["Key"])

            return keys
