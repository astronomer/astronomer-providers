import fnmatch
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from aiobotocore.client import AioBaseClient
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
    async def _check_exact_key(client: AioBaseClient, bucket: str, key: str) -> bool:
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
    async def _check_wildcard_key(client: AioBaseClient, bucket: str, wildcard_key: str) -> bool:
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

    async def check_key(self, client: AioBaseClient, bucket: str, key: str, wildcard_match: bool) -> bool:
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

    async def get_files(
        self,
        client: AioBaseClient,
        bucket: str,
        key: str,
        wildcard_match: bool,
        delimiter: Optional[str] = "/",
    ) -> List[Any]:
        """Gets a list of files in the bucket"""
        prefix = key
        if wildcard_match:
            prefix = re.split(r"[\[\*\?]", key, 1)[0]

        paginator = client.get_paginator("list_objects_v2")
        response = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
        keys: List[Any] = []
        async for page in response:
            if "Contents" in page:
                _temp = [k for k in page["Contents"] if isinstance(k.get("Size", None), (int, float))]
                keys = keys + _temp
        return keys

    @staticmethod
    async def _list_keys(
        client: AioBaseClient,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> List[str]:
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

        paginator = client.get_paginator("list_objects_v2")
        response = paginator.paginate(
            Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config
        )

        keys = []
        async for page in response:
            if "Contents" in page:
                for k in page["Contents"]:
                    keys.append(k["Key"])

        return keys

    async def is_keys_unchanged(
        self,
        client: AioBaseClient,
        bucket_name: str,
        prefix: str,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: Set[str] = set(),
        inactivity_seconds: int = 0,
        allow_delete: bool = True,
        last_activity_time: Optional[datetime] = None,
    ) -> Dict[str, str]:
        """
        Checks whether new objects have been uploaded and the inactivity_period
        has passed and updates the state of the sensor accordingly.
        :param current_objects: set of object ids in bucket during last poke.
        """
        list_keys = await self._list_keys(client=client, bucket_name=bucket_name, prefix=prefix)
        current_objects = set(list_keys)
        current_num_objects = len(current_objects)
        if current_num_objects > len(previous_objects):
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next poke.
            self.log.info(
                "New objects found at %s, resetting last_activity_time.",
                os.path.join(bucket_name, prefix),
            )
            self.log.debug("New objects: %s", current_objects - previous_objects)
            last_activity_time = datetime.now()
            inactivity_seconds = 0
            previous_objects = current_objects
            return {"status": "pending"}

        if len(previous_objects) - len(current_objects):
            # During the last poke interval objects were deleted.
            if allow_delete:
                deleted_objects = previous_objects - current_objects
                previous_objects = current_objects
                last_activity_time = datetime.now()
                self.log.info(
                    "Objects were deleted during the last poke interval. Updating the "
                    "file counter and resetting last_activity_time:\n%s",
                    deleted_objects,
                )
                return {"status": "pending"}

            return {
                "status": "error",
                "message": f" {os.path.join(bucket_name, prefix)} between pokes.",
            }

        if last_activity_time:
            inactivity_seconds = int((datetime.now() - last_activity_time).total_seconds())
        else:
            # Handles the first poke where last inactivity time is None.
            last_activity_time = datetime.now()
            inactivity_seconds = 0

        if inactivity_seconds >= inactivity_period:
            path = os.path.join(bucket_name, prefix)

            if current_num_objects >= min_objects:
                success_message = (
                    "SUCCESS: Sensor found %s objects at %s. "
                    "Waited at least %s seconds, with no new objects uploaded."
                )
                self.log.info(success_message, current_num_objects, path, inactivity_period)
                return {
                    "status": "success",
                    "message": success_message % (current_num_objects, path, inactivity_period),
                }

            self.log.error("FAILURE: Inactivity Period passed, not enough objects found in %s", path)
            return {
                "status": "error",
                "message": f"FAILURE: Inactivity Period passed, not enough objects found in {path}",
            }
        return {"status": "pending"}
