"""This module contains the Azure Data Factory hook's asynchronous implementation."""

from __future__ import annotations

import inspect
import warnings
from functools import wraps
from typing import Any, TypeVar, Union, cast

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from asgiref.sync import sync_to_async
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.datafactory.aio import DataFactoryManagementClient
from azure.mgmt.datafactory.models import PipelineRun

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]

T = TypeVar("T", bound=Any)


def get_field(extras: dict[str, Any], field_name: str, strict: bool = False) -> Any:
    """Get field from extra, first checking short name, then for backward compatibility we check for prefixed name."""
    backward_compatibility_prefix = "extra__azure_data_factory__"
    if field_name.startswith("extra__"):
        raise ValueError(
            f"Got prefixed name {field_name}; please remove the '{backward_compatibility_prefix}' prefix "
            "when using this method."
        )
    if field_name in extras:
        return extras[field_name] or None
    prefixed_name = f"{backward_compatibility_prefix}{field_name}"
    if prefixed_name in extras:
        return extras[prefixed_name] or None
    if strict:
        raise KeyError(f"Field {field_name} not found in extras")


def provide_targeted_factory_async(func: T) -> T:
    """
    Provide the targeted factory to the async decorated function in case it isn't specified.

    If ``resource_group_name`` or ``factory_name`` is not provided it defaults to the value specified in
    the connection extras.
    """
    signature = inspect.signature(func)

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        bound_args = signature.bind(*args, **kwargs)

        async def bind_argument(arg: Any, default_key: str) -> None:
            # Check if arg was not included in the function signature or, if it is, the value is not provided.
            if arg not in bound_args.arguments or bound_args.arguments[arg] is None:
                self = args[0]
                conn = await sync_to_async(self.get_connection)(self.conn_id)
                default_value = conn.extra_dejson.get(default_key)
                if not default_value:
                    raise AirflowException("Could not determine the targeted data factory.")

                bound_args.arguments[arg] = conn.extra_dejson[default_key]

        await bind_argument("resource_group_name", "extra__azure_data_factory__resource_group_name")
        await bind_argument("factory_name", "extra__azure_data_factory__factory_name")

        return await func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


class AzureDataFactoryHookAsync(AzureDataFactoryHook):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook` instead.
    """

    def __init__(self, azure_data_factory_conn_id: str):
        """Initialize the hook instance."""
        warnings.warn(
            (
                "This class is deprecated and will be removed in 2.0.0."
                "Use :class: `~airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        self._async_conn: DataFactoryManagementClient | None = None
        self.conn_id = azure_data_factory_conn_id
        super().__init__(azure_data_factory_conn_id=azure_data_factory_conn_id)

    async def get_async_conn(self) -> DataFactoryManagementClient:
        """Get async connection and connect to azure data factory."""
        if self._conn is not None:
            return cast(DataFactoryManagementClient, self._conn)  # pragma: no cover

        conn = await sync_to_async(self.get_connection)(self.conn_id)
        extras = conn.extra_dejson
        tenant = get_field(extras, "tenantId")

        try:
            subscription_id = get_field(extras, "subscriptionId", strict=True)
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

    @provide_targeted_factory_async
    async def get_pipeline_run(
        self,
        run_id: str,
        resource_group_name: str,
        factory_name: str,
        **config: Any,
    ) -> PipelineRun:
        """
        Connect to Azure Data Factory asynchronously to get the pipeline run details by run id.

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
        self, run_id: str, resource_group_name: str, factory_name: str
    ) -> str:
        """
        Connect to Azure Data Factory asynchronously and get the pipeline status by run_id.

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
            status: str = cast(str, pipeline_run.status)
            return status
        except Exception as e:
            raise AirflowException(e)
