import logging
from typing import Optional

from botocore.exceptions import ClientError

from astronomer_operators.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync

log = logging.getLogger(__name__)


class S3HookAsync(AwsBaseHookAsync):
    """
    Interact with AWS S3, using the aiobotocore library.
    """

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
        async_connection = await self.get_client_async()

        try:
            await async_connection.head_object(Bucket=bucket_name, Key=key)
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
        Checks that a key matching a wildcard expression exists in a bucket
        :param wildcard_key: the path to the key
        :type wildcard_key: str
        :param bucket_name: the name of the bucket
        :type bucket_name: str
        :param delimiter: the delimiter marks key hierarchy
        :type delimiter: str
        :return: True if a key exists and False if not.
        :rtype: bool
        """
        # TODO: Implement regex parser and search prefix in the list
        pass
