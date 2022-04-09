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
    """Interact with AWS S3, using the aiobotocore library."""

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

    async def list_prefixes(
        self,
        client: AioBaseClient,
        bucket_name: Optional[str] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
        max_items: Optional[int] = None,
    ) -> List[Any]:
        """
        Lists prefixes in a bucket under prefix

        :param client: ClientCreatorContext
        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :param page_size: pagination size
        :param max_items: maximum items to return
        :return: a list of matched prefixes
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

        prefixes = []
        async for page in response:
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    prefixes.append(common_prefix["Prefix"])

        return prefixes

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

    async def _check_for_prefix(
        self, client: AioBaseClient, prefix: str, delimiter: str, bucket_name: Optional[str] = None
    ) -> bool:
        return await self.check_for_prefix(
            client, prefix=prefix, delimiter=delimiter, bucket_name=bucket_name
        )

    async def check_for_prefix(
        self, client: AioBaseClient, prefix: str, delimiter: str, bucket_name: Optional[str] = None
    ) -> bool:
        """
        Checks that a prefix exists in a bucket

        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param delimiter: the delimiter marks key hierarchy.
        :return: False if the prefix does not exist in the bucket and True if it does.
        """
        prefix = prefix + delimiter if prefix[-1] != delimiter else prefix
        prefix_split = re.split(rf"(\w+[{delimiter}])$", prefix, 1)
        previous_level = prefix_split[0]
        plist = await self.list_prefixes(client, bucket_name, previous_level, delimiter)
        return prefix in plist

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
        previous_objects: Optional[Set[str]] = None,
        inactivity_seconds: int = 0,
        allow_delete: bool = True,
        last_activity_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Checks whether new objects have been uploaded and the inactivity_period
        has passed and updates the state of the sensor accordingly.

        :param client: aiobotocore client
        :param bucket_name: the name of the bucket
        :param prefix: a key prefix
        :param inactivity_period:  the total seconds of inactivity to designate
            keys unchanged. Note, this mechanism is not real time and
            this operator may not return until a poke_interval after this period
            has passed with no additional objects sensed.
        :param min_objects: the minimum number of objects needed for keys unchanged
            sensor to be considered valid.
        :param previous_objects: the set of object ids found during the last poke.
        :param inactivity_seconds: number of inactive seconds
        :param last_activity_time: last activity datetime
        :param allow_delete: Should this sensor consider objects being deleted
            between pokes valid behavior. If true a warning message will be logged
            when this happens. If false an error will be raised.
        :return: dictionary with status and message
        :rtype: Dict
        """
        if previous_objects is None:
            previous_objects = set()
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
            return {
                "status": "pending",
                "previous_objects": previous_objects,
                "last_activity_time": last_activity_time,
                "inactivity_seconds": inactivity_seconds,
            }

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
                return {
                    "status": "pending",
                    "previous_objects": previous_objects,
                    "last_activity_time": last_activity_time,
                    "inactivity_seconds": inactivity_seconds,
                }

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
        return {
            "status": "pending",
            "previous_objects": previous_objects,
            "last_activity_time": last_activity_time,
            "inactivity_seconds": inactivity_seconds,
        }
