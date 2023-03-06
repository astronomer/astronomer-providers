from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Sequence, cast

import requests
from airflow.exceptions import AirflowException, DagRunAlreadyExists
from airflow.models import XCom
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils import timezone
from airflow.utils.state import State
from requests.auth import AuthBase, HTTPBasicAuth

from astronomer.providers.core.triggers.external_dagrun import ExternalDeploymentDagRunTrigger
from astronomer.providers.utils.typing_compat import Context

XCOM_EXECUTION_DATE_ISO = "trigger_execution_date_iso"
XCOM_RUN_ID = "trigger_run_id"

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstanceKey


class ExternalDeploymentTriggerDagRunLink(BaseOperatorLink):
    """
    Operator link for ExternalDeploymentTriggerDagRunOperator.

    It allows users to access DAG triggered by task using ExternalDeploymentTriggerDagRunOperator.
    """

    name = "Triggered DAG"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        """Provide link to browse externally triggered Dag run."""
        # execution_date = XCom.get_value(ti_key=ti_key, key=XCOM_EXECUTION_DATE_ISO)
        run_id = XCom.get_value(ti_key=ti_key, key=XCOM_RUN_ID)

        operator = cast(ExternalDeploymentTriggerDagRunOperator, operator)
        dag_id = operator.trigger_dag_id
        http_conn_id = operator.http_conn_id
        endpoint = DAGRUN_ENDPOINT.format(dag_id=dag_id, run_id=run_id)
        hook = HttpHook(method="GET", http_conn_id=http_conn_id)

        return hook.url_from_endpoint(endpoint=endpoint)


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
        :param execution_date: Execution date for the dag (templated).
        :param reset_dag_run: Whether clear existing dag run if already exists.
            This is useful when backfill or rerun an existing dag run.
            This only resets (not recreates) the dag run.
            Dag run conf is immutable and will not be reset on rerun of an existing dag run.
            When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
            When reset_dag_run=True and dag run exists, existing dag run will be cleared to rerun.
        :param wait_for_completion: Whether wait for dag run completion. (default: False)
        :param poke_interval: Poke interval to check dag run status when wait_for_completion=True.
            (default: 60)
        :param allowed_states: List of allowed states, default is ``['success']``.
        :param failed_states: List of failed or dis-allowed states, default is ``None``.

    Changed in version 2.1.3: 'queued' is added as a possible value.
    """

    template_fields: Sequence[str] = (
        "headers",
        "trigger_dag_id",
        "trigger_run_id",
        "execution_date",
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
        conf: dict[str, Any] | None = None,
        execution_date: str | datetime | None = None,
        unpause_dag: bool = True,
        reset_dag_run: bool = False,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        allowed_states: list[Any] | None = None,
        failed_states: list[Any] | None = None,
        **kwargs,
    ) -> None:
        """Initialize the Operator."""
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
        self.trigger_run_id = trigger_run_id or ""
        self.conf = conf or {}
        self.execution_date = execution_date or timezone.utcnow()
        self.unpause_dag = unpause_dag
        self.reset_dag_run = reset_dag_run
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.failed_states = failed_states or [State.FAILED]

        try:
            json.dumps(self.conf)
        except TypeError as e:
            raise AirflowException("conf parameter should be JSON Serializable") from e

    def execute(self, context: dict[str, Any]) -> None:
        """
        Trigger a DAG run on an external deployment.
        :param context: Airflow context
        """
        execution_date = parse_execution_date(self.execution_date)

        api = AirflowApiClient(
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            headers=self.headers,
            extra_options=self.extra_options,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

        try:
            if self.unpause_dag:
                api.unpause_dag(dag_id=self.trigger_dag_id)

            dag_run = api.trigger_dag(
                dag_id=self.trigger_dag_id,
                run_id=self.trigger_run_id,
                conf=self.conf,
                execution_date=execution_date,
            )

        except DagRunAlreadyExists as e:
            if not self.reset_dag_run:
                raise e

            if not self.trigger_run_id:
                self.trigger_run_id = api.get_dag_run_by_date(
                    dag_id=self.trigger_dag_id, execution_date=execution_date
                ).run_id

            api.clear_dag_run(dag_id=self.trigger_dag_id, run_id=self.trigger_run_id)
            dag_run = api.get_dag_run(dag_id=self.trigger_dag_id, run_id=self.trigger_run_id)

        if dag_run is None:
            raise RuntimeError("The dag_run should be set here!")

        logging.info(f"/api/v1/dags/{dag_run.dag_id}/dagRuns/{dag_run.run_id}")

        self.defer(
            timeout=self.execution_timeout,
            trigger=ExternalDeploymentDagRunTrigger(
                http_conn_id=self.http_conn_id,
                method="GET",
                endpoint=f"/api/v1/dags/{dag_run.dag_id}/dagRuns/{dag_run.run_id}",
                data=None,
                headers=self.headers,
                extra_options=self.extra_options,
                poke_interval=self.poke_interval,
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.

        :param context: Airflow context
        :param event: Trigger Event
        """
        logging.info(event)
        state = event.get("state")
        dag_id = event.get("dag_id")
        run_id = event.get("run_id")
        execution_date = event.get("execution_date")
        if state in self.allowed_states:
            self.log.info(f"Dag Run Succeeded with response: {event['state']}")
            if run_id:
                context["ti"].xcom_push(key=XCOM_RUN_ID, value=run_id)
            if execution_date:
                context["ti"].xcom_push(key=XCOM_EXECUTION_DATE_ISO, value=execution_date)
            return
        raise AirflowException(f"{dag_id} failed with failed state {state}")


#######################################
def parse_execution_date(execution_date: datetime | str) -> datetime:
    """
    Parse execution date to datetime object.

    :param execution_date: Execution date
    :return: Execution date as datetime object
    """
    if execution_date is not None and not isinstance(execution_date, (str, datetime)):
        raise TypeError(
            f"Expected str or datetime.datetime type for execution_date.Got {type(execution_date)}"
        )
    if isinstance(execution_date, datetime):
        return execution_date
    elif isinstance(execution_date, str):
        return timezone.parse(execution_date)


def parse_dag(json_dict: dict[str, Any]) -> DAG:
    """
    Parse JSON dictionary to DAG object.

    Can be used directly with json object_hook. E.g. : json.loads(..., object_hook=parse_dag)

    :param json_dict: Dictionary containing Dag properties to parse
    """
    dag = DAG(**json_dict)
    logging.debug(f"Decoded Dag: {dag}")
    return dag


def parse_dag_run(json_dict: dict[str, Any]) -> DagRun:
    """
    Parse JSON dictionary to DagRun object.

    Can be used directly with json object_hook. E.g. : json.loads(..., object_hook=parse_dag_run)

    :param json_dict: Dictionary containing DagRun properties to parse
    """
    data_interval_start = json_dict.get("data_interval_start")
    data_interval_end = json_dict.get("data_interval_end")

    dag_run = DagRun(
        dag_id=json_dict.get("dag_id"),
        run_id=json_dict.get("dag_run_id"),
        queued_at=json_dict.get("queued_at"),
        execution_date=json_dict.get("execution_date"),
        start_date=json_dict.get("start_date"),
        external_trigger=json_dict.get("external_trigger"),
        conf=json_dict.get("conf"),
        state=json_dict.get("state"),
        run_type=json_dict.get("run_type"),
        dag_hash=json_dict.get("dag_hash"),
        creating_job_id=json_dict.get("creating_job_id"),
        data_interval=(
            timezone.parse(data_interval_start) if data_interval_start else None,
            timezone.parse(data_interval_end) if data_interval_end else None,
        ),
    )
    logging.debug(f"Decoded Dag Run: {dag_run}")
    return dag_run


DAG_ENDPOINT = "/api/v1/dags/{dag_id}"
DAGRUN_ENDPOINT = "/api/v1/dags/{dag_id}/dagRuns/{run_id}"
DAGRUNS_ENDPOINT = "/api/v1/dags/{dag_id}/dagRuns"
CLEAR_DAGRUN_ENDPOINT = "/api/v1/dags/{dag_id}/dagRuns/{run_id}/clear"


class AirflowApiClient:
    """Airflow API Client."""

    def __init__(
        self,
        http_conn_id: str,
        auth_type: type[AuthBase],
        headers: dict[str, Any],
        extra_options: dict[str, Any],
        tcp_keep_alive: bool,
        tcp_keep_alive_idle: int,
        tcp_keep_alive_count: int,
        tcp_keep_alive_interval: int,
    ) -> None:
        self.http_conn_id = http_conn_id
        self.auth_type = auth_type
        self.headers = headers
        self.extra_options = extra_options
        self.tcp_keep_alive = tcp_keep_alive
        self.tcp_keep_alive_idle = tcp_keep_alive_idle
        self.tcp_keep_alive_count = tcp_keep_alive_count
        self.tcp_keep_alive_interval = tcp_keep_alive_interval

    def call_api(
        self, endpoint: str, method: str, data: dict | str | None, extra_options: dict
    ) -> requests.Response:
        """
        Call the External Airflow API Endpoint.

        :param endpoint: Airflow API endpoint to be called i.e. /api/v1/dags/
        :param method: the API method to be called
        :param data: payload to be uploaded or request parameters
        :param extra_options: additional options to be used when executing the request
            i.e. {'check_response': False} to avoid checking raising exceptions on non
            2XX or 3XX status codes
        :return: the API response
        """
        hook = HttpHook(
            method=method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

        logging.debug(f"Endpoint: {endpoint}")
        logging.debug(f"Data: {data}")
        logging.debug(f"Headers: {self.headers}")
        logging.debug(f"Extras: {extra_options}")

        # Do not raise_for_status if Dagrun already exist
        _extra_options = extra_options | {"check_response": False}
        response = hook.run(endpoint, data, self.headers, _extra_options)
        if response.status_code != 409:
            hook.check_response(response)

        logging.debug(f"Response: {response.status_code} - {response.json()}")

        return response

    def trigger_dag(
        self, dag_id: str, run_id: str, execution_date: str | datetime, conf: dict[str, Any]
    ) -> DagRun:
        """Trigger a Dag Run."""
        if isinstance(execution_date, datetime):
            execution_date_str = execution_date.isoformat()
        else:
            execution_date_str = execution_date

        logging.info(f"Triggering Dag {dag_id}")
        endpoint = DAGRUNS_ENDPOINT.format(dag_id=dag_id)
        data = {"dag_run_id": run_id, "execution_date": execution_date_str, "conf": conf}

        response = self.call_api(endpoint, "POST", json.dumps(data), self.extra_options)
        logging.info(response.json())
        if response.status_code == 409:
            raise DagRunAlreadyExists(
                DagRun(dag_id=dag_id, run_id=run_id, execution_date=execution_date), execution_date, run_id
            )

        return response.json(object_hook=parse_dag_run)

    def get_dag_run(self, dag_id: str, run_id: str) -> DagRun:
        """Fetch a Dag Run from its dag_id and dag_run_id."""
        logging.info(f"Get Dag run {run_id} for DAG {dag_id}")
        endpoint = DAGRUN_ENDPOINT.format(dag_id=dag_id, run_id=run_id)
        response = self.call_api(endpoint, "GET", None, self.extra_options)
        return response.json(object_hook=parse_dag_run)

    def get_dag_run_by_date(self, dag_id: str, execution_date: datetime | str) -> DagRun:
        """Fetch a Dag Run from its dag_id and execution_date."""
        if isinstance(execution_date, datetime):
            execution_date = execution_date.isoformat()

        logging.info(f"Get Dag run by date {execution_date} for DAG {dag_id}")
        endpoint = DAGRUNS_ENDPOINT.format(dag_id=dag_id)
        data = {
            "execution_date_gte": execution_date,
            "execution_date_lte": execution_date,
        }
        response = self.call_api(endpoint, "GET", data, self.extra_options)
        resp_json = response.json()
        return parse_dag_run(resp_json.get("dag_runs")[0])

    def clear_dag_run(self, dag_id: str, run_id: str) -> None:
        """Clear a Dag Run based on dag_id and dag_run_id.
        :param dag_id: Dag ID
        :param run_id: Dag Run ID
        """
        logging.info(f"Clearing Dag run {run_id} for DAG {dag_id}")
        endpoint = CLEAR_DAGRUN_ENDPOINT.format(dag_id=dag_id, run_id=run_id)
        data = {"dry_run": False}
        self.call_api(endpoint, "POST", json.dumps(data), self.extra_options)

    def unpause_dag(self, dag_id: str) -> None:
        """Unpause a Dag based on dag_id.
        :param dag_id: Dag ID
        """
        logging.info(f"Unpausing Dag {dag_id}")
        endpoint = DAG_ENDPOINT.format(dag_id=dag_id)
        data = {"is_paused": False}
        self.call_api(endpoint, "PATCH", json.dumps(data), self.extra_options)
