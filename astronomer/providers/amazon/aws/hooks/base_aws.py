from aiobotocore.client import AioBaseClient
from aiobotocore.session import get_session
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper
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
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
    :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
    :param config: Configuration for botocore client.

    .. seealso::

        `AWS API <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html>`_
    """

    async def get_client_async(self) -> AioBaseClient:
        """Create an Async Client object to communicate with AWS services."""
        # Fetch the Airflow connection object
        connection_object = await sync_to_async(self.get_connection)(self.aws_conn_id)

        conn_config = AwsConnectionWrapper(
            conn=connection_object,
            region_name=self.region_name,
            botocore_config=self.config,
            verify=self.verify,
        )

        async_connection = get_session()
        return async_connection.create_client(
            service_name=self.client_type,
            region_name=conn_config.region_name,
            aws_secret_access_key=conn_config.aws_secret_access_key,
            aws_access_key_id=conn_config.aws_access_key_id,
            aws_session_token=conn_config.aws_session_token,
            verify=self.verify,
            config=self.config,
        )
