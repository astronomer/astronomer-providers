from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

import requests
from aiohttp import ClientSession
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AstroHook(BaseHook):
    """
    Custom Apache Airflow Hook for interacting with Astro Cloud API.

    :param astro_cloud_conn_id: The connection ID to retrieve Astro Cloud credentials.
    """

    conn_name_attr = "astro_cloud_conn_id"
    default_conn_name = "astro_cloud_default"
    conn_type = "Astro Cloud"
    hook_name = "Astro Cloud"

    def __init__(self, astro_cloud_conn_id: str = "astro_cloud_conn_id"):
        super().__init__()
        self.astro_cloud_conn_id = astro_cloud_conn_id

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
        base_url = conn.host or os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")
        if base_url is None:
            raise AirflowException(f"Airflow host is missing in connection {self.astro_cloud_conn_id}")
        token = conn.password
        if token is None:
            raise AirflowException(f"Astro API token is missing in connection {self.astro_cloud_conn_id}")
        return base_url, token

    @property
    def _headers(self) -> dict[str, str]:
        """Generates and returns headers for Astro Cloud API requests."""
        _, token = self.get_conn()
        headers = {"accept": "application/json", "Authorization": f"Bearer {token}"}
        return headers

    def get_dag_runs(self, external_dag_id: str) -> list[dict[str, str]]:
        """
        Retrieves information about running or queued DAG runs.

        :param external_dag_id: External ID of the DAG.
        """
        base_url, _ = self.get_conn()
        path = f"/api/v1/dags/{external_dag_id}/dagRuns"
        params: dict[str, int | str | list[str]] = {
            "limit": 1,
            "state": ["running", "queued"],
            "order_by": "-execution_date",
        }
        url = f"{base_url}{path}"
        response = requests.get(url, headers=self._headers, params=params)
        response.raise_for_status()
        data: dict[str, list[dict[str, str]]] = response.json()
        return data["dag_runs"]

    def get_dag_run(self, external_dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
        """
        Retrieves information about a specific DAG run.

        :param external_dag_id: External ID of the DAG.
        :param dag_run_id: ID of the DAG run.
        """
        base_url, _ = self.get_conn()
        dag_run_id = quote(dag_run_id)
        path = f"/api/v1/dags/{external_dag_id}/dagRuns/{dag_run_id}"
        url = f"{base_url}{path}"
        response = requests.get(url, headers=self._headers)
        response.raise_for_status()
        dr: dict[str, Any] = response.json()
        return dr

    async def get_a_dag_run(self, external_dag_id: str, dag_run_id: str) -> dict[str, Any] | None:
        """
        Retrieves information about a specific DAG run.

        :param external_dag_id: External ID of the DAG.
        :param dag_run_id: ID of the DAG run.
        """
        base_url, _ = self.get_conn()
        dag_run_id = quote(dag_run_id)
        path = f"/api/v1/dags/{external_dag_id}/dagRuns/{dag_run_id}"
        url = f"{base_url}{path}"

        async with ClientSession(headers=self._headers) as session:
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
        base_url, _ = self.get_conn()
        dag_run_id = quote(dag_run_id)
        path = f"/api/v1/dags/{external_dag_id}/dagRuns/{dag_run_id}/taskInstances/{external_task_id}"
        url = f"{base_url}{path}"
        response = requests.get(url, headers=self._headers)
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
        base_url, _ = self.get_conn()
        dag_run_id = quote(dag_run_id)
        path = f"/api/v1/dags/{external_dag_id}/dagRuns/{dag_run_id}/taskInstances/{external_task_id}"
        url = f"{base_url}{path}"

        async with ClientSession(headers=self._headers) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                ti: dict[str, Any] = await response.json()
                return ti
