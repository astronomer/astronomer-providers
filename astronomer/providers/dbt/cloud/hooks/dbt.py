from abc import ABC
from typing import Any, List, Optional

from airflow import AirflowException
from airflow.compat.functools import cached_property
from airflow.models import Connection
from airflow.providers.dbt.cloud.hooks.dbt import TokenAuth, fallback_to_default_account
from asgiref.sync import sync_to_async

from astronomer.providers.http.hooks.http import HttpHookAsync


class DbtCloudHookAsync(HttpHookAsync, ABC):
    """
    Interact with dbt Cloud using the V2 API.

    :param dbt_cloud_conn_id: The ID of the :ref:`dbt Cloud connection <howto/connection:dbt-cloud>`.
    """

    conn_name_attr = "dbt_cloud_conn_id"
    default_conn_name = "dbt_cloud_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"

    def __init__(self, dbt_cloud_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(auth_type=TokenAuth)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        tenant = self.connection.schema if self.connection.schema else "cloud"

        self.base_url = f"https://{tenant}.getdbt.com/api/v2/accounts/"

    @cached_property
    async def connection(self) -> Connection:
        """Get the DBT cloud connection details async"""
        _connection = await sync_to_async(self.get_connection)(self.dbt_cloud_conn_id)
        if not _connection.password:
            raise AirflowException("An API token is required to connect to dbt Cloud.")
        return _connection

    async def get_conn_details(self):
        """Get the auth details from the connection details"""
        _headers = {}
        auth = self.auth_type(self.connection.password)
        return _headers, auth

    @fallback_to_default_account
    async def get_job_status(
        self, run_id: int, account_id: Optional[int] = None, include_related: Optional[List[str]] = None
    ) -> Any:
        """
        Uses Http async call to retrieves metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        """
        self.method = "GET"
        job_run = await self.run(
            endpoint=f"{account_id}/runs/{run_id}/", data={"include_related": include_related}
        )
        job_run_status = job_run.json()["data"]["status"]
        return job_run_status
