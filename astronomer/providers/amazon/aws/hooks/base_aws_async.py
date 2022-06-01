from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook, _parse_s3_config
from asgiref.sync import sync_to_async


class AwsBaseHookAsync(AwsBaseHook):
    """
    Interacts with AWS using aiobotocore asynchronously.

    .. note::
        AwsBaseHookAsync uses aiobotocore to create asynchronous S3 hooks. Hence, AwsBaseHookAsync
        only supports the authentication mechanism that aiobotocore supports. The ability to assume
        roles provided in the Airflow connection extra args via aiobotocore is not supported by the
        library yet.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param verify: Whether or not to verify SSL certificates.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
    :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
    :param config: Configuration for botocore client.
        (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
    """

    async def get_client_async(self) -> AioBaseClient:
        """Create an Async Client object to communicate with AWS services."""
        # Fetch the Airflow connection object
        connection_object = await sync_to_async(self.get_connection)(self.aws_conn_id)
        extra_config = connection_object.extra_dejson

        aws_access_key_id = None
        aws_secret_access_key = None
        aws_session_token = None
        if connection_object.login:
            aws_access_key_id = connection_object.login
            aws_secret_access_key = connection_object.password
            self.log.info("Credentials retrieved from login")
        elif "aws_access_key_id" in extra_config and "aws_secret_access_key" in extra_config:
            aws_access_key_id = extra_config["aws_access_key_id"]
            aws_secret_access_key = extra_config["aws_secret_access_key"]
            aws_session_token = extra_config.get("aws_session_token")
            self.log.info("Credentials retrieved from extra_config")
        elif "s3_config_file" in extra_config:
            aws_access_key_id, aws_secret_access_key = await sync_to_async(_parse_s3_config)(
                extra_config["s3_config_file"],
                extra_config.get("s3_config_format"),
                extra_config.get("profile"),
            )
            self.log.info("Credentials retrieved from extra_config['s3_config_file']")
        else:
            self.log.info("No credentials retrieved from Connection")

        region_name = self.region_name
        if self.region_name is None and "region_name" in extra_config:
            self.log.info("Retrieving region_name from Connection.extra_config['region_name']")
            region_name = extra_config["region_name"]

        async_connection = get_session()
        return async_connection.create_client(
            service_name=self.client_type,
            region_name=region_name,
            aws_secret_access_key=aws_secret_access_key,
            aws_access_key_id=aws_access_key_id,
            aws_session_token=aws_session_token,
            verify=self.verify,
            config=self.config,
        )
