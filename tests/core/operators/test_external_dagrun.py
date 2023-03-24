import json
from unittest import mock

import pytest
from airflow.exceptions import DagRunAlreadyExists
from airflow.models import DAG, DagRun
from airflow.utils.state import DagRunState, State
from airflow.utils.timezone import datetime
from requests.auth import HTTPBasicAuth
from requests.models import Response

from astronomer.providers.core.operators.external_trigger_dagrun import (
    # ExternalDeploymentTriggerDagRunOperator,
    AirflowApiClient,
    parse_execution_date,
)


@pytest.mark.parametrize(
    "input, expected, exception",
    [
        ("2023-01-01", datetime(2023, 1, 1), None),
        (datetime(2023, 1, 1), datetime(2023, 1, 1), None),
        (None, None, TypeError),
    ],
)
def test_parse_execution_date(input, expected, exception):
    """Assert parse_execution_date method return datetime object"""
    if not input:
        with pytest.raises(exception):
            parse_execution_date(input)
    else:
        assert expected == parse_execution_date(input)


@pytest.mark.parametrize(
    "input, expected, exception",
    [
        (
            {
                "dag_id": "test-dag",
                "start_date": "2023-01-01",
                "schedule_interval": "@daily",
                "default_args": {"owner": "airflow"},
            },
            DAG(
                dag_id="test-dag",
                start_date=datetime(2023, 1, 1),
                schedule_interval="@daily",
                default_args={"owner": "airflow"},
            ),
            None,
        ),
    ],
)
def test_parse_dag(input, expected, exception):
    if not input:
        with pytest.raises(exception):
            parse_execution_date(input)
    else:
        assert expected == parse_execution_date(input)


@pytest.mark.parametrize(
    "input, expected, exception",
    [
        {
            "conf": {},
            "dag_id": "test-dag",
            "dag_run_id": "test_run",
            "data_interval_end": "2023-01-01T00:00:00+00:00",
            "data_interval_start": "2022-12-31T00:00:00+00:00",
            "end_date": "2023-03-07T19:06:36.866560+00:00",
            "execution_date": "2023-01-01T00:00:00+00:00",
            "external_trigger": True,
            "last_scheduling_decision": "2023-01-01T00:06:36.863502+00:00",
            "logical_date": "2023-01-01T00:00:00+00:00",
            "note": None,
            "run_type": "manual",
            "start_date": "2023-01-01T00:06:30.873256+00:00",
            "state": "success",
        },
        DagRun(
            dag_id="test-dag",
            run_id="test_run",
            queued_at=None,
            execution_date=datetime(2023, 1, 1),
            start_date=datetime(2023, 1, 1, 0, 6, 30, 873256),
            external_trigger=True,
            conf={},
            state=State.SUCCESS,
            data_interval=(datetime(2022, 12, 31), datetime(2023, 1, 1)),
        ),
        None,
    ],
)
def test_parse_dag_run(input, expected, exception):
    if not input:
        with pytest.raises(exception):
            parse_execution_date(input)
    else:
        assert expected == parse_execution_date(input)


@pytest.fixture()
def api_client():
    return AirflowApiClient(
        http_conn_id="http_default",
        auth_type=HTTPBasicAuth,
        headers={"Content-Type": "application/json"},
        extra_options={},
        tcp_keep_alive=5,
        tcp_keep_alive_idle=5,
        tcp_keep_alive_count=5,
        tcp_keep_alive_interval=5,
    )


@pytest.fixture()
def api_response():
    return {
        "conf": {},
        "dag_id": "test_dag",
        "dag_run_id": "test_run",
        "data_interval_end": "2023-01-01T00:00:00+00:00",
        "data_interval_start": "2022-12-31T00:00:00+00:00",
        "end_date": None,
        "execution_date": "2023-01-01T00:00:00+00:00",
        "external_trigger": True,
        "last_scheduling_decision": None,
        "logical_date": "2023-01-01T00:00:00+00:00",
        "note": None,
        "run_type": "manual",
        "start_date": None,
        "state": "queued",
    }


@pytest.fixture()
def dagrun():
    return DagRun(
        dag_id="test_dag",
        run_id="test_run",
        execution_date=datetime(2023, 1, 1),
        start_date=None,
        external_trigger=True,
        conf={},
        state=DagRunState.QUEUED,
        run_type="manual",
        data_interval=(datetime(2022, 12, 31), datetime(2023, 1, 1)),
    )


class TestAirflowApiClient:
    def test_init(self):
        http_conn_id = ("http_default",)
        auth_type = (HTTPBasicAuth,)
        headers = ({"Content-Type": "application/json"},)
        extra_options = ({},)

        api = AirflowApiClient(
            http_conn_id=http_conn_id,
            auth_type=auth_type,
            headers=headers,
            extra_options=extra_options,
        )
        assert api.http_conn_id == http_conn_id
        assert api.auth_type == auth_type
        assert api.headers == headers
        assert api.extra_options == extra_options
        # assert api.tcp_keep_alive == 5
        # assert api.tcp_keep_alive_idle == 5
        # assert api.tcp_keep_alive_count == 5
        # assert api.tcp_keep_alive_interval == 5

    @mock.patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_call_api(self, api_client, mock_run):
        endpoint = "test-endpoint"
        method = "GET"
        data = None

        api_client.call_api(
            endpoint=endpoint,
            method=method,
            data=data,
        )

        mock_run.assert_called_once_with(
            endpoint=endpoint, method=method, data=data, extra_options={"check_response": False}
        )

    # TODO: @pytest.mark.parametrize()
    @mock.patch("astronomer.providers.core.operator.externaltrigger_dagrun.AirflowApiClient.call_api")
    def test_trigger_dag_run(self, api_client, json_response, dagrun, mock_call_api):
        endpoint = "/api/v1/dags/{dag_id}/dagRuns".format(dag_id="test_dag")

        mock_response = mock.Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = json_response

        data = {"dag_run_id": "test_run", "execution_date": "2023-01-01", "conf": {}}

        assert dagrun == api_client.trigger_dag_run("test_dag", "test_run", "2023-01-01", conf={})
        mock_call_api.assert_called_once_with(endpoint, "POST", json.dumps(data))

    def test_trigger_dag_run_already_exists(self, api_client):
        with pytest.raises(DagRunAlreadyExists):
            api_client.trigger_dag_run("test-dag", "test_run", "2023-01-01", conf={})

    @mock.patch("astronomer.providers.core.operator.externaltrigger_dagrun.AirflowApiClient.call_api")
    def test_get_dag_run(self, api_client, dagrun, json_response, mock_call_api):
        endpoint = "/api/v1/dags/{dag_id}/dagRuns".format(dag_id="test_dag")

        mock_response = mock.Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = json_response

        data = {"dag_run_id": "test_run", "execution_date": "2023-01-01", "conf": {}}

        mock_call_api.assert_called_once_with(endpoint, "POST", json.dumps(data))
        assert api_client.get_dag_run("test-dag", "test_run") == dagrun

    @mock.patch("astronomer.providers.core.operator.externaltrigger_dagrun.AirflowApiClient.call_api")
    def test_get_dag_run_by_date(self, api_client, dagrun, json_response, mock_call_api):
        endpoint = "/api/v1/dags/{dag_id}/dagRuns".format(dag_id="test_dag")

        mock_response = mock.Mock(spec=Response)
        mock_response.status_code = 200
        mock_response.json.return_value = json_response

        data = {"dag_run_id": "test_run", "execution_date": "2023-01-01", "conf": {}}

        mock_call_api.assert_called_once_with(endpoint, "POST", json.dumps(data))
        assert api_client.get_dag_run_by_date("test-dag", "2023-01-01") == dagrun

    @mock.patch("astronomer.providers.core.operator.externaltrigger_dagrun.AirflowApiClient.call_api")
    def test_clear_dag_run(self, api_client, dagrun, json_response, mock_call_api):
        endpoint = "/api/v1/dags/{dag_id}/dagRuns/{run_id}/clear".format(dag_id="test_dag", run_id="test_run")

        data = {"dry_run": False}

        mock_call_api.assert_called_once_with(endpoint, "POST", json.dumps(data))

    @mock.patch("astronomer.providers.core.operator.externaltrigger_dagrun.AirflowApiClient.call_api")
    def test_unpause_dag(self, api_client, dagrun, json_response, mock_call_api):
        endpoint = "/api/v1/dags/{dag_id}".format(dag_id="test_dag")
        data = {"is_paused": False}
        mock_call_api.assert_called_once_with(endpoint, "PATCH", json.dumps(data))


# class TestExternalDeploymentTriggerDagRunOperator:


# class MockResponse:
#     def __init__(self, json_data: dict[str, Any], status_code:int = 200):
#         self.json_data = json_data
#         self.status_code = status_code
#
#     def json(self):
#         return self.json_data


#     CONN_ID = "http_default"
#     TRIGGER_DAG_ID = "test-dag"
#     TRIGGER_RUN_ID = "test-run"
#     CONF = {}
#     HEADERS = {"Content-Type": "application/json"}
#
#
#
#
#
#
#
#
#     @pytest.mark.asyncio
#     @mock.patch("astronomer.providers.core.operators.external_trigger_dagrun.AirflowApiClient.call_api",)
#     def test_external_deployment_run(self, mocked_response):
#         """Assert execute method defer for external deployment operator run status sensors"""
#         mocked_response.return_value = MockResponse(
#             {
#                 "conf": {},
#                 "dag_id": "example_dag_basic",
#                 "dag_run_id": "manual__2023-01-01T00:00:00+00:00",
#                 "data_interval_end": "2023-01-01T00:00:00+00:00",
#                 "data_interval_start": "2023-01-01T00:00:00+00:00",
#                 "end_date": None,
#                 "execution_date": "2023-01-01T00:00:00+00:00",
#                 "external_trigger": None,
#                 "last_scheduling_decision": None,
#                 "logical_date": "2023-01-01T00:00:00+00:00",
#                 "note": None,
#                 "run_type": "manual",
#                 "start_date": None,
#                 "state": "queued"
#             }
#         )
#
#         operator = ExternalDeploymentTriggerDagRunOperator(
#             task_id="external_deployment_trigger_dag_run",
#             http_conn_id=self.CONN_ID,
#             trigger_dag_id=self.TRIGGER_DAG_ID,
#             trigger_run_id=self.TRIGGER_RUN_ID,
#             conf=self.CONF,
#             execution_date=datetime(2023, 1, 1),
#             unpause_dag=True,
#             reset_dag_run=True,
#             wait_for_completion=True,
#             headers=self.HEADERS,
#             poke_interval=5,
#         )
#         with pytest.raises(TaskDeferred) as exc:
#             operator.execute({})
#         assert isinstance(
#             exc.value.trigger, ExternalDeploymentDagRunTrigger
#         ), "Trigger is not a ExternalDeploymentDagRunTrigger"
#
#     @pytest.mark.parametrize(
#         "mock_state, mock_message",
#         [("success", "Dag Run Succeeded with response: %s")],
#     )
#     def test_external_deployment_execute_complete_success(self, mock_state, mock_message):
#         """Assert execute_complete log success message when trigger fire with target state"""
#         operator = ExternalDeploymentTriggerDagRunOperator(
#             task_id="external_deployment_trigger_dag_run",
#             http_conn_id=self.CONN_ID,
#             trigger_dag_id=self.TRIGGER_DAG_ID,
#             trigger_run_id=self.TRIGGER_RUN_ID,
#             conf=self.CONF,
#             execution_date=datetime(2023, 1, 1),
#             unpause_dag=True,
#             reset_dag_run=True,
#             wait_for_completion=True,
#             headers=self.HEADERS,
#             poke_interval=5,
#         )
#
#         with mock.patch.object(operator.log, "info") as mock_log_info:
#             operator.execute_complete(context={}, event={"state": mock_state})
#         mock_log_info.assert_called_with(mock_message, {"state": mock_state})
#
#     @pytest.mark.parametrize(
#         "mock_resp, mock_message",
#         [
#             ({"state": "error"}, "Task Failed with response: %s"),
#             ({"test": "test"}, "Task Failed with response: %s"),
#         ],
#     )
#     def test_external_deployment_execute_complete_value_error(self, mock_resp, mock_message):
#         """Assert execute_complete method to raise Value error"""
#         operator = ExternalDeploymentTriggerDagRunOperator(
#             task_id="external_deployment_trigger_dag_run",
#             http_conn_id=self.CONN_ID,
#             trigger_dag_id=self.TRIGGER_DAG_ID,
#             trigger_run_id=self.TRIGGER_RUN_ID,
#             conf=self.CONF,
#             execution_date=datetime(2023, 1, 1),
#             unpause_dag=True,
#             reset_dag_run=True,
#             wait_for_completion=True,
#             headers=self.HEADERS,
#             poke_interval=5,
#         )
#         with pytest.raises(ValueError):
#             operator.execute_complete(context={}, event=mock_resp)
