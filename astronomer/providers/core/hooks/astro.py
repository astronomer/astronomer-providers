from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import requests
from aiohttp import ClientSession
from airflow.exceptions import AirflowException

from astronomer.providers.utils.compat import BaseHook


class AstroHook(BaseHook):
    """
    Custom Apache Airflow Hook for interacting with Astro Cloud API.

    Works against Airflow 2 and Airflow 3 deployments. The first request probes the deployment for the
    Airflow 3 REST API (``/api/v2``) and falls back to the Airflow 2 one (``/api/v1``) when it is missing.

    :param astro_cloud_conn_id: The connection ID to retrieve Astro Cloud credentials.
    """

    conn_name_attr = "astro_cloud_conn_id"
    default_conn_name = "astro_cloud_default"
    conn_type = "Astro Cloud"
    hook_name = "Astro Cloud"

    def __init__(self, astro_cloud_conn_id: str = "astro_cloud_conn_id"):
        super().__init__()
        self.astro_cloud_conn_id = astro_cloud_conn_id
        self._api_version: str | None = None

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """
        Returns UI field behavior customization for the Astro Cloud connection.

        This method defines hidden fields, relabeling, and placeholders for UI display.
        """
        return {
            "hidden_fields": ["login", "port", "schema", "extra"],
            "relabeling": {
                "password": "Astro Cloud API Token",
            },
            "placeholders": {
                "host": "https://clmkpsyfc010391acjie00t1l.astronomer.run/d5lc9c9x",
                "password": "Astro API JWT Token",
            },
        }

    def get_conn(self) -> tuple[str, str]:
        """Retrieves the Astro Cloud connection details."""
        conn = BaseHook.get_connection(self.astro_cloud_conn_id)
        base_url = (
            conn.host
            or os.environ.get("AIRFLOW__API__BASE_URL")
            or os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")
        )
        if base_url is None:
            raise AirflowException(f"Airflow host is missing in connection {self.astro_cloud_conn_id}")
        token = conn.password
        if token is None:
            raise AirflowException(f"Astro API token is missing in connection {self.astro_cloud_conn_id}")
        return base_url, token

    def _base_url_and_headers(self) -> tuple[str, dict[str, str]]:
        """Return the base URL and request headers from a single connection lookup."""
        base_url, token = self.get_conn()
        return base_url, {"accept": "application/json", "Authorization": f"Bearer {token}"}

    @property
    def _headers(self) -> dict[str, str]:
        """Generates and returns headers for Astro Cloud API requests."""
        _, headers = self._base_url_and_headers()
        return headers

    def _get_api_version(self, base_url: str, headers: dict[str, str]) -> str:
        """Return the REST API version the deployment serves, probing it on first use."""
        if self._api_version is None:
            response = requests.get(f"{base_url}/api/v2/version", headers=headers)
            if response.status_code == 404:  # Airflow 2 has no /api/v2, and Airflow 3 removed /api/v1.
                self._api_version = "v1"
            else:
                response.raise_for_status()
                self._api_version = "v2"
        return self._api_version

    async def _get_api_version_async(self, session: ClientSession, base_url: str) -> str:
        """Return the REST API version the deployment serves, probing it on first use."""
        if self._api_version is None:
            async with session.get(f"{base_url}/api/v2/version") as response:
                if response.status == 404:
                    self._api_version = "v1"
                else:
                    response.raise_for_status()
                    self._api_version = "v2"
        return self._api_version

    @staticmethod
    def _dag_run_url(base_url: str, api_version: str, external_dag_id: str, dag_run_id: str) -> str:
        return f"{base_url}/api/{api_version}/dags/{external_dag_id}/dagRuns/{quote(dag_run_id)}"

    def get_dag_runs(self, external_dag_id: str) -> list[dict[str, str]]:
        """
        Retrieves information about running or queued DAG runs.

        :param external_dag_id: External ID of the DAG.
        """
        base_url, headers = self._base_url_and_headers()
        api_version = self._get_api_version(base_url, headers)
        url = f"{base_url}/api/{api_version}/dags/{external_dag_id}/dagRuns"
        params: dict[str, int | str | list[str]] = {
            "limit": 1,
            "state": ["running", "queued"],
            # Airflow 3 runs can have no logical date, but every run has run_after.
            "order_by": "-run_after" if api_version == "v2" else "-execution_date",
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data: dict[str, list[dict[str, str]]] = response.json()
        return data["dag_runs"]

    def get_dag_run(self, external_dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
        """
        Retrieves information about a specific DAG run.

        :param external_dag_id: External ID of the DAG.
        :param dag_run_id: ID of the DAG run.
        """
        base_url, headers = self._base_url_and_headers()
        url = self._dag_run_url(
            base_url, self._get_api_version(base_url, headers), external_dag_id, dag_run_id
        )
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        dr: dict[str, Any] = response.json()
        return dr

    async def get_a_dag_run(self, external_dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
        """
        Retrieves information about a specific DAG run.

        :param external_dag_id: External ID of the DAG.
        :param dag_run_id: ID of the DAG run.
        """
        base_url, headers = self._base_url_and_headers()
        async with ClientSession(headers=headers) as session:
            api_version = await self._get_api_version_async(session, base_url)
            url = self._dag_run_url(base_url, api_version, external_dag_id, dag_run_id)
            async with session.get(url) as response:
                response.raise_for_status()
                dr: dict[str, Any] = await response.json()
                return dr

    def get_task_instance(
        self, external_dag_id: str, dag_run_id: str, external_task_id: str
    ) -> dict[str, Any] | None:
        """
        Retrieves information about a specific task instance within a DAG run.

        :param external_dag_id: External ID of the DAG.
        :param dag_run_id: ID of the DAG run.
        :param external_task_id: External ID of the task.
        """
        base_url, headers = self._base_url_and_headers()
        dag_run_url = self._dag_run_url(
            base_url, self._get_api_version(base_url, headers), external_dag_id, dag_run_id
        )
        response = requests.get(f"{dag_run_url}/taskInstances/{external_task_id}", headers=headers)
        response.raise_for_status()
        ti: dict[str, Any] = response.json()
        return ti

    async def get_a_task_instance(
        self, external_dag_id: str, dag_run_id: str, external_task_id: str
    ) -> dict[str, Any] | None:
        """
        Retrieves information about a specific task instance within a DAG run.

        :param external_dag_id: External ID of the DAG.
        :param dag_run_id: ID of the DAG run.
        :param external_task_id: External ID of the task.
        """
        base_url, headers = self._base_url_and_headers()
        async with ClientSession(headers=headers) as session:
            api_version = await self._get_api_version_async(session, base_url)
            dag_run_url = self._dag_run_url(base_url, api_version, external_dag_id, dag_run_id)
            async with session.get(f"{dag_run_url}/taskInstances/{external_task_id}") as response:
                response.raise_for_status()
                ti: dict[str, Any] = await response.json()
                return ti
