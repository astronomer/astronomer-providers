from functools import wraps
from inspect import signature
from typing import Any, Dict, List, Optional, Tuple, TypeVar, cast

import aiohttp
from aiohttp import ClientResponseError
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from asgiref.sync import sync_to_async

from astronomer.providers.package import get_provider_info

T = TypeVar("T", bound=Any)


def provide_account_id(func: T) -> T:
    """
    Decorator which provides a fallback value for ``account_id``. If the ``account_id`` is None or not passed
    to the decorated function, the value will be taken from the configured dbt Cloud Airflow Connection.
    """
    function_signature = signature(func)

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        bound_args = function_signature.bind(*args, **kwargs)

        if bound_args.arguments.get("account_id") is None:
            self = args[0]
            if self.dbt_cloud_conn_id:
                connection = await sync_to_async(self.get_connection)(self.dbt_cloud_conn_id)
                default_account_id = connection.login
                if not default_account_id:
                    raise AirflowException("Could not determine the dbt Cloud account.")
                bound_args.arguments["account_id"] = int(default_account_id)

        return await func(*bound_args.args, **bound_args.kwargs)

    return cast(T, wrapper)


class DbtCloudHookAsync(BaseHook):
    """
    Interact with dbt Cloud using the V2 API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    """

    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    def __init__(self, dbt_cloud_conn_id: str):
        self.dbt_cloud_conn_id = dbt_cloud_conn_id

    async def get_headers_tenants_from_connection(self) -> Tuple[Dict[str, Any], str]:
        """Get Headers, tenants from the connection details"""
        headers: Dict[str, Any] = {}
        connection: Connection = await sync_to_async(self.get_connection)(self.dbt_cloud_conn_id)
        tenant: str = connection.schema if connection.schema else "cloud"
        provider_info = get_provider_info()
        package_name = provider_info["package-name"]
        version = provider_info["versions"]
        headers["User-Agent"] = f"{package_name}-v{version}"
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = f"Token {connection.password}"
        return headers, tenant

    @staticmethod
    def get_request_url_params(
        tenant: str, endpoint: str, include_related: Optional[List[str]] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Form URL from base url and endpoint url

        :param tenant: The tenant name which is need to be replaced in base url.
        :param endpoint: Endpoint url to be requested.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        data: Dict[str, Any] = {}
        base_url = f"https://{tenant}.getdbt.com/api/v2/accounts/"
        if include_related:
            data = {"include_related": include_related}
        if base_url and not base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            url = base_url + "/" + endpoint
        else:
            url = (base_url or "") + (endpoint or "")
        return url, data

    @provide_account_id
    async def get_job_details(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> Any:
        """
        Uses Http async call to retrieve metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        endpoint = f"{account_id}/runs/{run_id}/"
        headers, tenant = await self.get_headers_tenants_from_connection()
        url, params = self.get_request_url_params(tenant, endpoint, include_related)
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, params=params) as response:
                try:
                    response.raise_for_status()
                    return await response.json()
                except ClientResponseError as e:
                    raise AirflowException(str(e.status) + ":" + e.message)

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
        try:
            self.log.info("Getting the status of job run %s.", str(run_id))
            response = await self.get_job_details(
                run_id, account_id=account_id, include_related=include_related
            )
            job_run_status: int = response["data"]["status"]
            return job_run_status
        except Exception as e:
            raise e
