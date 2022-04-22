from typing import Any, Optional, Union

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from asgiref.sync import sync_to_async
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory.aio import DataFactoryManagementClient
from azure.mgmt.datafactory.models import PipelineRun

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


class AzureDataFactoryHookAsync(AzureDataFactoryHook):
    """
    An Async Hook connects to Azure DataFactory to perform pipeline operations

    :param azure_data_factory_conn_id: The :ref:`Azure Data Factory connection id<howto/connection:adf>`.
    """

    def __init__(self, azure_data_factory_conn_id: str):
        self._async_conn: DataFactoryManagementClient = None
        self.conn_id = azure_data_factory_conn_id
        super().__init__(azure_data_factory_conn_id=azure_data_factory_conn_id)

    async def get_async_conn(self) -> DataFactoryManagementClient:
        """Get async connection and connect to azure data factory"""
        if self._conn is not None:
            return self._conn

        conn = await sync_to_async(self.get_connection)(self.conn_id)
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

        return DataFactoryManagementClient(
            credential=credential,
            subscription_id=subscription_id,
        )

    async def get_pipeline_run(
        self,
        run_id: str,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
        **config: Any,
    ) -> PipelineRun:
        """
        Connects to Azure Data Factory asynchronously to get the pipeline run details by run id

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        """
        async with await self.get_async_conn() as client:
            try:
                pipeline_run = await client.pipeline_runs.get(resource_group_name, factory_name, run_id)
                return pipeline_run
            except Exception as e:
                raise AirflowException(e)

    async def get_adf_pipeline_run_status(
        self, run_id: str, resource_group_name: Optional[str] = None, factory_name: Optional[str] = None
    ) -> str:
        """
        Connects to Azure Data Factory asynchronously and gets the pipeline status by run_id

        :param run_id: The pipeline run identifier.
        :param resource_group_name: The resource group name.
        :param factory_name: The factory name.
        """
        try:
            pipeline_run = await self.get_pipeline_run(
                run_id=run_id,
                factory_name=factory_name,
                resource_group_name=resource_group_name,
            )
            status: str = pipeline_run.status
            return status
        except Exception as e:
            raise AirflowException(e)
