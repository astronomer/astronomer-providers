import logging
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from astronomer.providers.amazon.aws.triggers.s3 import S3KeyTrigger

log = logging.getLogger(__name__)


class S3KeySensorAsync(BaseOperator):
    """
    Waits for a key (a file-like instance on S3) to be present in a S3 bucket
    asynchronously. S3 being a key/value it does not support folders. The path
    is just a key a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :type bucket_key: str
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :type bucket_name: str
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :type wildcard_match: bool
    :param aws_conn_id: a reference to the s3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    """

    def __init__(
        self,
        *,
        bucket_key: str,
        bucket_name: Optional[str] = None,
        wildcard_match: bool = False,
        aws_conn_id: str = "aws_default",
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: Optional[S3Hook] = None

    def _resolve_bucket_and_key(self) -> None:
        """If key is URI, parse bucket"""
        if self.bucket_name is None:
            self.bucket_name, self.bucket_key = S3Hook.parse_s3_url(self.bucket_key)
        else:
            parsed_url = urlparse(self.bucket_key)
            if parsed_url.scheme != "" or parsed_url.netloc != "":
                raise AirflowException("If bucket_name provided, bucket_key must be relative path, not URI.")

    def execute(self, context: Dict) -> Any:
        self._resolve_bucket_and_key()
        self.defer(
            timeout=self.execution_timeout,
            trigger=S3KeyTrigger(
                bucket_name=self.bucket_name,  # type: ignore
                bucket_key=self.bucket_key,
                wildcard_match=self.wildcard_match,
                aws_conn_id=self.aws_conn_id,
                verify=self.verify,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict, event=None):  # pylint: disable=unused-argument
        return None
