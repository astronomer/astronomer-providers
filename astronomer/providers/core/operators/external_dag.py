from __future__ import annotations

import datetime
import json
import time
from typing import TYPE_CHECKING, Any, Optional, Sequence

import requests
from airflow.exceptions import (
    AirflowException,
    DagNotFound,
    DagRunAlreadyExists,
    DagRunNotFound,
)
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.dagrun import DagRun
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.state import State
from requests.auth import AuthBase, HTTPBasicAuth

XCOM_EXECUTION_DATE_ISO = "trigger_execution_date_iso"
XCOM_RUN_ID = "trigger_run_id"


if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey


def _dag_run_decoder(json_dict: dict) -> DagRun:
    """
    Decode JSON dictionary to DagRun
    To be used with json.loads(..., object_hook=_dag_run_decoder)
    :param json_dict: Dictionary containing DagRun properties to decode
    """
    return DagRun(
        dag_id=json_dict.get("dag_id"),
        run_id=json_dict.get("dag_run_id"),
        queued_at=None,
        execution_date=json_dict.get("logical_date"),
        start_date=json_dict.get("start_date"),
        external_trigger=json_dict.get("external_trigger"),
        conf=json_dict.get("conf"),
        state=json_dict.get("state"),
        run_type=json_dict.get("run_type"),
        dag_hash=None,
        creating_job_id=None,
        data_interval=(json_dict.get("data_interval_start"), json_dict.get("data_interval_end")),
    )


class ExternalDeploymentTriggerDagRunLink(BaseOperatorLink):
    """
    Operator link for ExternalDeploymentTriggerDagRunOperator.
    It allows users to access DAG triggered by task using ExternalDeploymentTriggerDagRunOperator.
    """

    name = "Triggered DAG"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        # TODO: Figure out how to achieve this.
        # Potential issues:
        #  - version compatibility
        #  - /api/v1/config not always accessible
        #  - Can't use build_airflow_url_with_query()
        pass


class ExternalDeploymentTriggerDagRunOperator(BaseOperator):
    """
        External Deployment Trigger Dag Run Operator Make HTTP call to trigger a Dag Run and poll for the response state of externally
        deployed DAG run to complete. The host should be external deployment url, header must contain access token

        .. seealso::
            - `Retrieve an access token and Deployment URL <https://docs.astronomer.io/astro/airflow-api#step-1-retrieve-an-access-token-and-deployment-url.>`_

        :param http_conn_id: The HTTP Connection ID to run the operator against
        :param headers: The HTTP headers to be added to the GET request
        :param extra_options: Extra options for the 'requests' library, see the
            'requests' documentation (options to modify timeout, ssl, etc.)
        :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
        :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
        :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
        :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
            ``socket.TCP_KEEPINTVL``)
        :param trigger_dag_id: The dag_id to trigger (templated).
        :param trigger_run_id: The run ID to use for the triggered DAG run (templated).
            If not provided, a run ID will be automatically generated.
        :param conf: Configuration for the DAG run (templated).
        :param logical_date: Execution date for the dag (templated).
        :param reset_dag_run: Whether clear existing dag run if already exists.
            This is useful when backfill or rerun an existing dag run.
            This only resets (not recreates) the dag run.
            Dag run conf is immutable and will not be reset on rerun of an existing dag run.
            When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
            When reset_dag_run=True and dag run exists, existing dag run will be cleared to rerun.
        :param wait_for_completion: Whether or not wait for dag run completion. (default: False)
        :param poke_interval: Poke interval to check dag run status when wait_for_completion=True.
            (default: 60)
        :param note: Manually entered notes by the user about the DagRun.
        :param allowed_states: List of allowed states, default is ``['success']``.
        :param failed_states: List of failed or dis-allowed states, default is ``None``.

    Changed in version 2.1.3: 'queued' is added as a possible value.
    """

    template_fields: Sequence[str] = (
        "endpoint",
        "headers",
        "trigger_dag_id",
        "trigger_run_id",
        "logical_date",
        "conf",
    )
    template_fields_renderers = {"headers": "json", "data": "py", "conf": "py"}
    ui_color = "#7352ba"
    operator_extra_links = [ExternalDeploymentTriggerDagRunLink()]

    def __init__(
        self,
        *,
        http_conn_id: str = "http_default",
        headers: dict[str, str],
        extra_options: dict[str, Any] | None = None,
        auth_type: type[AuthBase] = HTTPBasicAuth,
        tcp_keep_alive: bool = True,
        tcp_keep_alive_idle: int = 120,
        tcp_keep_alive_count: int = 20,
        tcp_keep_alive_interval: int = 30,
        trigger_dag_id: str,
        trigger_run_id: str | None = None,
        conf: dict | None = None,
        logical_date: str | datetime.datetime | None = None,
        reset_dag_run: bool = False,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        allowed_states: list | None = None,
        failed_states: list | None = None,
        note: str = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.headers = headers
        self.extra_options = extra_options or {}
        self.auth_type = auth_type
        self.tcp_keep_alive = tcp_keep_alive
        self.tcp_keep_alive_idle = tcp_keep_alive_idle
        self.tcp_keep_alive_count = tcp_keep_alive_count
        self.tcp_keep_alive_interval = tcp_keep_alive_interval
        self.trigger_dag_id = trigger_dag_id
        self.trigger_run_id = trigger_run_id
        self.conf = conf
        self.reset_dag_run = reset_dag_run
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.failed_states = failed_states or [State.FAILED]
        self.note = note

        if logical_date is not None and not isinstance(logical_date, (str, datetime.datetime)):
            raise TypeError(
                f"Expected str or datetime.datetime type for execution_date.Got {type(logical_date)}"
            )

        self.logical_date = logical_date

        if isinstance(self.logical_date, datetime.datetime):
            self.parsed_logical_date = self.logical_date
        elif isinstance(self.logical_date, str):
            self.parsed_logical_date = timezone.parse(self.logical_date)
        else:
            self.parsed_logical_date = timezone.utcnow()

        try:
            json.dumps(self.conf)
        except TypeError:
            raise AirflowException("conf parameter should be JSON Serializable")

    def _call_airflow_api(self, endpoint: str, method: str, data: Optional[dict] = None) -> requests.Response:
        hook = HttpHook(
            method=method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

        response = hook.run(endpoint, data, self.headers, self.extra_options)

        response.raise_for_status()

        return response

    def _check_dag_exists(self) -> bool:
        endpoint = f"/api/v1/dags/{self.trigger_dag_id}"

        response = self._call_airflow_api(endpoint, "GET")

        if response.json().get("dag_id"):
            return True
        else:
            return False

    def _trigger_dag(self) -> DagRun:
        endpoint = f"/api/v1/dags/{self.trigger_dag_id}/dagRuns"
        run_id = self.trigger_run_id if self.trigger_run_id else None
        data = {
            "dag_id": self.trigger_dag_id,
            "dag_run_id": run_id,
            "logical_date": self.parsed_logical_date,
            "state": State.QUEUED,
            "conf": self.conf,
            "note": self.note,
        }
        response = self._call_airflow_api(endpoint, "POST", data)

        dag_run = response.json(object_hook=_dag_run_decoder)

        return dag_run

    def _get_dag_run(self) -> DagRun:
        endpoint = (
            f"/api/v1/dags/{self.trigger_dag_id}/dagRuns/{self.trigger_run_id}"
        )

        response = self._call_airflow_api(endpoint, "GET", None)

        resp_json = response.json()

        dag_runs = resp_json.get("dag_runs")
        if not dag_runs:
            raise DagRunNotFound
        dag_run = _dag_run_decoder(dag_runs[0])

        return dag_run

    def _get_dag_run_by_date(self) -> DagRun:
        endpoint = f"/api/v1/dags/{self.trigger_dag_id}/dagRuns"

        data = {
            "execution_date_gte": self.parsed_logical_date,
            "execution_date_lte": self.parsed_logical_date,
        }
        response = self._call_airflow_api(endpoint, "GET", data)

        resp_json = response.json()
        dag_run = _dag_run_decoder(resp_json.get("dag_runs")[0])

        return dag_run

    def _clear_dag_run(self, dag_run_id: str) -> DagRun:
        endpoint = f"/api/v1/dags/{self.trigger_dag_id}/dagRuns/{dag_run_id}/clear"
        response = self._call_airflow_api(endpoint, "POST", None)

        dag_run = response.json(object_hook=_dag_run_decoder)

        return dag_run

    def execute(self, context: Context):
        if not self._check_dag_exists():
            raise DagNotFound
        try:
            dag_run = self._trigger_dag()

        except DagRunAlreadyExists as e:
            if self.reset_dag_run:
                self.log.info("Clearing %s on %s", self.trigger_dag_id, self.parsed_logical_date)

                if self.trigger_run_id:
                    trigger_run_id = self.trigger_run_id
                else:
                    trigger_run_id = self._get_dag_run_by_date().run_id

                dag_run = self._clear_dag_run(trigger_run_id)

            else:
                raise e
        if dag_run is None:
            raise RuntimeError("The dag_run should be set here!")

        # Store the execution date from the dag run (either created or found above) to
        # be used when creating the extra link on the webserver.
        ti = context["task_instance"]
        ti.xcom_push(key=XCOM_EXECUTION_DATE_ISO, value=dag_run.execution_date.isoformat())
        ti.xcom_push(key=XCOM_RUN_ID, value=dag_run.run_id)

        if self.wait_for_completion:
            while True:
                self.log.info(
                    "Waiting for %s on %s to become allowed state %s ...",
                    self.trigger_dag_id,
                    dag_run.execution_date,
                    self.allowed_states,
                )
                time.sleep(self.poke_interval)

                dag_run = self._get_dag_run()

                state = dag_run.state
                if state in self.failed_states:
                    raise AirflowException(f"{self.trigger_dag_id} failed with failed states {state}")
                if state in self.allowed_states:
                    self.log.info("%s finished with allowed state %s", self.trigger_dag_id, state)
                    return
