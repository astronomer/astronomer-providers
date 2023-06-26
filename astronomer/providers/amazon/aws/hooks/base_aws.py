from __future__ import annotations

from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession, get_session
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper
from asgiref.sync import sync_to_async


class AwsBaseHookAsync(AwsBaseHook):
    """
    Interacts with AWS using aiobotocore asynchronously.

    .. note::
        AwsBaseHookAsync uses aiobotocore to create asynchronous hooks. Hence, AwsBaseHookAsync
        only supports the authentication mechanism that aiobotocore supports. Currently, AwsBaseHookAsync supports
        only AWS STS client method ``assume_role`` provided in the Airflow connection extra args via aiobotocore.

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
        connection_object = await sync_to_async(self.get_connection)(self.aws_conn_id)  # type: ignore[arg-type]

        conn_config = AwsConnectionWrapper(
            conn=connection_object,
            region_name=self.region_name,
            botocore_config=self.config,
            verify=self.verify,
        )

        async_connection = get_session()
        session_token = conn_config.aws_session_token
        aws_secret = conn_config.aws_secret_access_key
        aws_access = conn_config.aws_access_key_id
        if conn_config.role_arn:
            credentials = await self.get_role_credentials(
                async_session=async_connection, conn_config=conn_config
            )
            if credentials:
                session_token = credentials["SessionToken"]
                aws_access = credentials["AccessKeyId"]
                aws_secret = credentials["SecretAccessKey"]
        return async_connection.create_client(
            service_name=self.client_type,
            region_name=conn_config.region_name,
            aws_secret_access_key=aws_secret,
            aws_access_key_id=aws_access,
            aws_session_token=session_token,
            verify=self.verify,
            config=self.config,
            endpoint_url=conn_config.endpoint_url,
        )

    @staticmethod
    async def get_role_credentials(
        async_session: AioSession, conn_config: AwsConnectionWrapper
    ) -> dict[str, str] | None:
        """Get the role_arn, method credentials from connection details and get the role credentials detail"""
        async with async_session.create_client(
            "sts",
            aws_access_key_id=conn_config.aws_access_key_id,
            aws_secret_access_key=conn_config.aws_secret_access_key,
        ) as client:
            return_response = None
            if conn_config.assume_role_method == "assume_role" or conn_config.assume_role_method is None:
                response: dict[str, dict[str, str]] = await client.assume_role(
                    RoleArn=conn_config.role_arn,
                    RoleSessionName="RoleSession",
                    **conn_config.assume_role_kwargs,
                )
                return_response = response["Credentials"]
            return return_response
