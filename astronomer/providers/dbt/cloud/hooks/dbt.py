from abc import ABC
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from airflow import AirflowException
from airflow.compat.functools import cached_property
from airflow.models import Connection
from airflow.providers.dbt.cloud.hooks.dbt import TokenAuth, fallback_to_default_account
from asgiref.sync import sync_to_async

from astronomer.providers.http.hooks.http import HttpHookAsync

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


def _get_provider_info() -> Tuple[str, str]:
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    package_name = manager.hooks[DbtCloudHookAsync.conn_type].package_name  # type: ignore[union-attr]
    provider = manager.providers[package_name]

    return package_name, provider.version


class DbtCloudHookAsync(HttpHookAsync, ABC):
    """
    Interact with dbt Cloud using the V2 API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    """

    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    def __init__(self, dbt_cloud_conn_id: str = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(auth_type=TokenAuth)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.http_conn_id = ""

    @cached_property
    async def dbt_connection(self) -> "Connection":
        """Get the DBT cloud connection details async"""
        _connection = await sync_to_async(self.get_connection)(self.dbt_cloud_conn_id)
        if not _connection.password:
            raise AirflowException("An API token is required to connect to dbt Cloud.")

        tenant = _connection.schema if _connection.schema else "cloud"
        self.base_url = f"https://{tenant}.getdbt.com/api/v2/accounts/"

        return _connection

    async def get_conn_details(self) -> Tuple[Dict[str, Any], Any]:
        """Get the auth and headers details from the connection details"""
        _headers = {}
        dbt_connection = await self.dbt_connection
        auth = self.auth_type(dbt_connection.password)
        package_name, provider_version = _get_provider_info()
        _headers["User-Agent"] = f"{package_name}-v{provider_version}"
        _headers["Content-Type"] = "application/json"
        _headers["Authorization"] = f"Token {dbt_connection.password}"
        return _headers, auth

    @fallback_to_default_account
    async def get_job_details(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> "ClientResponse":
        """
        Uses Http async call to retrieve metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        self.method = "GET"
        headers, auth = await self.get_conn_details()
        data: Dict[str, Any] = {}
        if include_related:
            data = {"include_related": include_related}
        response = await self.run(endpoint=f"{account_id}/runs/{run_id}/", data=data, headers=headers)
        return response

    async def get_job_status(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> int:
        """
        Retrieves the status for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        self.log.info("Getting the status of job run %s.", str(run_id))

        job_run = await self.get_job_details(account_id=account_id, run_id=run_id)
        response = await job_run.json()
        job_run_status = response["data"]["status"]

        return job_run_status
