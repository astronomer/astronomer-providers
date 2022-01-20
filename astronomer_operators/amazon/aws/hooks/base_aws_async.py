import logging

from aiobotocore.session import get_session
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook, _parse_s3_config

log = logging.getLogger(__name__)


class AwsBaseHookAsync(AwsBaseHook):
    """
    Interacts with AWS using aiobotocore
    """

    def get_client_async(self):
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
        return async_connection.create_client(
            self.resource_type,
            region_name=self.region_name,
            aws_secret_access_key=aws_secret_access_key,
            aws_access_key_id=aws_access_key_id,
        )
