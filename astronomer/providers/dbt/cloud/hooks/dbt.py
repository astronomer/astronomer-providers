from abc import ABC
from functools import wraps
from inspect import signature
from typing import Any, Dict, List, Optional, Tuple, TypeVar, cast

import aiohttp
from aiohttp import ClientResponseError
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from asgiref.sync import sync_to_async

T = TypeVar("T", bound=Any)


def _get_provider_info() -> Tuple[str, str]:
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    package_name = manager.hooks[DbtCloudHookAsync.conn_type].package_name  # type: ignore[union-attr]
    provider = manager.providers[package_name]

    return package_name, provider.version


def provide_account_id(func: T) -> T:
    """
    Function decorator that provides a bucket name taken from the connection
    in case no bucket name has been passed to the function.
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


class DbtCloudHookAsync(BaseHook, ABC):
    """
    Interact with dbt Cloud using the V2 API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    """

    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    def __init__(self, dbt_cloud_conn_id: str):
        self.connection = dbt_cloud_conn_id
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.base_url = ""

    async def get_headers(self) -> Dict[str, Any]:
        """Get Headers, base url from the connection details"""
        headers: Dict[str, Any] = {}
        self.connection: Connection = await sync_to_async(self.get_connection)(self.dbt_cloud_conn_id)
        print(self.connection)
        tenant = self.connection.schema if self.connection.schema else "cloud"
        self.base_url = f"https://{tenant}.getdbt.com/api/v2/accounts/"
        package_name, provider_version = _get_provider_info()
        headers["User-Agent"] = f"{package_name}-v{provider_version}"
        headers["Content-Type"] = "application/json"
        headers["Authorization"] = f"Token {self.connection.password}"
        return headers

    def get_request_url_params(
        self, endpoint: str, include_related: Optional[List[str]] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """Form URL from base url and endpoint url"""
        data: Dict[str, Any] = {}
        if include_related:
            data = {"include_related": include_related}
        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            url = self.base_url + "/" + endpoint
        else:
            url = (self.base_url or "") + (endpoint or "")
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
        headers = await self.get_headers()
        url, params = self.get_request_url_params(endpoint, include_related)
        print("headers ", headers)
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
            response = await self.get_job_details(account_id=account_id, run_id=run_id)
            job_run_status: int = response["data"]["status"]
            return job_run_status
        except Exception as e:
            raise e
