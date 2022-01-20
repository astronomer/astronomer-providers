import logging
from typing import Optional, Union

from aiobotocore.session import get_session
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook, _parse_s3_config
from botocore.config import Config

log = logging.getLogger(__name__)


class AwsBaseHookAsync(AwsBaseHook):
    """
    Interacts with AWS using aiobotocore
    """

    conn_name_attr = "aws_conn_id"
    default_conn_name = "aws_default"
    conn_type = "aws"
    hook_name = "Amazon Web Services"

    def __init__(
        self,
        aws_conn_id: Optional[str] = default_conn_name,
        verify: Union[bool, str, None] = None,
        region_name: Optional[str] = None,
        client_type: Optional[str] = None,
        resource_type: Optional[str] = None,
        config: Optional[Config] = None,
    ) -> None:
        super().__init__()
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.client_type = client_type
        self.resource_type = resource_type
        self.region_name = region_name
        self.config = config

        if not (self.client_type or self.resource_type):
            raise AirflowException("Either client_type or resource_type must be provided.")

    async def get_client_async(self):
        # Fetch the Airflow connection object
        connection_object = self.get_connection(self.aws_conn_id)
        extra_config = connection_object.extra_dejson

        aws_access_key_id = None
        aws_secret_access_key = None
        if connection_object.login:
            aws_access_key_id = connection_object.login
            aws_secret_access_key = connection_object.password
            self.log.info("Credentials retrieved from login")
        elif "aws_access_key_id" in extra_config and "aws_secret_access_key" in extra_config:
            aws_access_key_id = extra_config["aws_access_key_id"]
            aws_secret_access_key = extra_config["aws_secret_access_key"]
            self.log.info("Credentials retrieved from extra_config")
        elif "s3_config_file" in extra_config:
            aws_access_key_id, aws_secret_access_key = _parse_s3_config(
                extra_config["s3_config_file"],
                extra_config.get("s3_config_format"),
                extra_config.get("profile"),
            )
            self.log.info("Credentials retrieved from extra_config['s3_config_file']")
        else:
            self.log.info("No credentials retrieved from Connection")

        async_connection = get_session()
        async with async_connection.create_client(
            self.resource_type,
            region_name=self.region_name,
            aws_secret_access_key=aws_secret_access_key,
            aws_access_key_id=aws_access_key_id,
        ) as client:
            return client
