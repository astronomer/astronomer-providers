from typing import Optional, Union

from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory.aio import DataFactoryManagementClient

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


class AzureDataFactoryHookAsync(AzureDataFactoryHook):
    """Async AzureDataFactoryHook connects to azure datafactory and execute the pipeline and monitor the pipeline"""

    def __init__(self, azure_data_factory_conn_id: str):
        self._async_conn: DataFactoryManagementClient = None
        self.conn_id = azure_data_factory_conn_id
        super().__init__()

    def get_async_conn(self):
        """Get async connection and connect to azure data factory"""
        if self._conn is not None:
            return self._conn

        conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get("extra__azure_data_factory__tenantId")

        try:
            subscription_id = conn.extra_dejson["extra__azure_data_factory__subscriptionId"]
        except KeyError:
            raise ValueError("A Subscription ID is required to connect to Azure Data Factory.")

        credential: Credentials
        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=tenant
            )
        else:
            credential = DefaultAzureCredential()
        self._async_conn = self._create_client(credential, subscription_id)

        return self._async_conn

    @staticmethod
    def _create_async_client(credential: Credentials, subscription_id: str):
        return DataFactoryManagementClient(
            credential=credential,
            subscription_id=subscription_id,
        )

    async def get_pipeline_run_status(
        self, run_id: str, resource_group_name: Optional[str] = None, factory_name: Optional[str] = None
    ) -> str:
        """
        Connect to azure Data factory and gets the pipeline status by run_id

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        """
        pipeline_run_status = self.get_pipeline_run(
            run_id=run_id,
            factory_name=factory_name,
            resource_group_name=resource_group_name,
        ).status

        return pipeline_run_status
