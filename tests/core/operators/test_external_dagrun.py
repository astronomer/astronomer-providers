import json
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import patch

import pytest
from airflow.exceptions import DagRunAlreadyExists
from airflow.models import DagRun
from airflow.utils.state import DagRunState, State
from requests.auth import HTTPBasicAuth
from requests.models import Response

from astronomer.providers.core.operators.external_trigger_dagrun import (  # ExternalDeploymentTriggerDagRunOperator,
    AirflowApiClient,
    parse_dag_run,
    parse_execution_date,
)


@pytest.fixture
def api_client() -> AirflowApiClient:
    return AirflowApiClient(
        http_conn_id="http_default",
        auth_type=HTTPBasicAuth,
        headers={"Content-Type": "application/json"},
        extra_options={},
        tcp_keep_alive=True,
        tcp_keep_alive_idle=5,
        tcp_keep_alive_count=5,
        tcp_keep_alive_interval=5,
    )


@pytest.fixture
def dagrun() -> DagRun:
    return DagRun(
        dag_id="test_dag",
        run_id="test_run",
        execution_date=datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        start_date=None,
        external_trigger=True,
        conf={},
        state=DagRunState.QUEUED,
        run_type="manual",
        data_interval=(datetime(2022, 12, 31), datetime(2023, 1, 1)),
        queued_at=None,
    )


@pytest.mark.parametrize(
    "input, expected, exception",
    [
        ("2023-01-01T00:00:00+00:00", datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc), None),
        (
            datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            None,
        ),
        (None, None, TypeError),
    ],
)
def test_parse_execution_date(input, expected, exception):
    """Assert parse_execution_date method return datetime object"""
    if input is None:
        with pytest.raises(exception):
            parse_execution_date(input)
    else:
        assert expected == parse_execution_date(input)


@pytest.mark.parametrize(
    "input, expected, exception",
    [
        (
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
                execution_date=datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                start_date=datetime(2023, 1, 1, 0, 6, 30, 873256, tzinfo=timezone.utc),
                external_trigger=True,
                conf={},
                state=State.SUCCESS,
                data_interval=(
                    datetime(2022, 12, 31, 0, 0, 0, tzinfo=timezone.utc),
                    datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                ),
            ),
            None,
        )
    ],
)
def test_parse_dag_run(input, expected, exception):
    assert isinstance(parse_dag_run(input), DagRun)


class TestAirflowApiClient:
    def test_init(self):
        http_conn_id = "http_default"
        auth_type = HTTPBasicAuth
        headers = {"Content-Type": "application/json"}
        extra_options = {}

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
    def test_call_api(self, mock_run, api_client):
        endpoint = "test-endpoint"
        method = "GET"
        data = None

        api_client.call_api(
            endpoint=endpoint,
            method=method,
            data=data,
        )

        mock_run.assert_called_once_with(
            endpoint=endpoint,
            data=data,
            headers={"Content-Type": "application/json"},
            extra_options={"check_response": False},
        )

    def test_trigger_dag_run(self, api_client):
        response = mock.Mock(spec=Response, autospec=True)
        response.status_code = 200
        response.json.return_value = {
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
        with patch.object(api_client, "call_api", return_value=response) as mock_call_api:
            endpoint = "/api/v1/dags/{dag_id}/dagRuns".format(dag_id="test_dag")
            data = {"dag_run_id": "test_run", "execution_date": "2023-01-01T00:00:00+00:00", "conf": {}}
            api_client.trigger_dag_run(
                "test_dag", "test_run", datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc), conf={}
            )
        mock_call_api.assert_called_once_with(endpoint, "POST", json.dumps(data))

    def test_trigger_dag_run_already_exists(self, api_client):
        response = mock.Mock(spec=Response)
        response.status_code = 409
        response.json.return_value = {
            "detail": "DAGRun with DAG ID: 'example_dag_basic' and DAGRun logical date: '2023-01-01 00:00:00+00:00' already exists",
            "status": 409,
            "title": "Conflict",
            "type": "Errors/AlreadyExists",
        }
        with patch.object(api_client, "call_api", return_value=response):
            with pytest.raises(DagRunAlreadyExists):
                api_client.trigger_dag_run(
                    "test_dag", "test_run", datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc), conf={}
                )

    def test_get_dag_run(self, api_client, dagrun):
        response = mock.Mock(spec=Response)
        response.status_code = 200
        response.json.return_value = {
            "conf": {},
            "dag_id": "test_dag",
            "dag_run_id": "test_run",
            "data_interval_end": "2023-01-01T00:00:00+00:00",
            "data_interval_start": "2022-12-31T00:00:00+00:00",
            "end_date": "2023-01-01T00:01:35.000000+00:00",
            "execution_date": "2023-01-01T00:00:00+00:00",
            "external_trigger": True,
            "last_scheduling_decision": "2023-01-01T00:01:00.000000+00:00",
            "logical_date": "2023-01-01T00:00:00+00:00",
            "note": None,
            "run_type": "manual",
            "start_date": "2023-01-01T00:01:30.000000+00:00",
            "state": "success",
        }

        with patch.object(api_client, "call_api", return_value=response) as mock_call_api:
            endpoint = "/api/v1/dags/{dag_id}/dagRuns/{dag_run}".format(dag_id="test-dag", dag_run="test_run")
            api_client.get_dag_run("test-dag", "test_run")
            # TODO: Add assert to compare DagRun objects
        mock_call_api.assert_called_once_with(endpoint, "GET", None)

    def test_get_dag_run_by_date(self, api_client, dagrun):
        response = mock.Mock(spec=Response)
        response.status_code = 200
        response.json.return_value = {
            "dag_runs": [
                {
                    "conf": {},
                    "dag_id": "test_dag",
                    "dag_run_id": "test_run",
                    "data_interval_end": "2023-01-01T00:00:00+00:00",
                    "data_interval_start": "2022-12-31T00:00:00+00:00",
                    "end_date": "2023-01-01T00:01:35.000000+00:00",
                    "execution_date": "2023-01-01T00:00:00+00:00",
                    "external_trigger": True,
                    "last_scheduling_decision": "2023-01-01T00:01:00.000000+00:00",
                    "logical_date": "2023-01-01T00:00:00+00:00",
                    "note": None,
                    "run_type": "manual",
                    "start_date": "2023-01-01T00:01:30.000000+00:00",
                    "state": "success",
                }
            ]
        }
        with patch.object(api_client, "call_api", return_value=response) as mock_call_api:
            endpoint = "/api/v1/dags/{dag_id}/dagRuns".format(dag_id="test-dag")
            api_client.get_dag_run_by_date("test-dag", "2023-01-01")
            execution_date = "2023-01-01"
            data = {
                "execution_date_gte": execution_date,
                "execution_date_lte": execution_date,
            }
            # TODO: Add assert to compare DagRun objects
        mock_call_api.assert_called_once_with(endpoint, "GET", data)

    def test_clear_dag_run(self, api_client, dagrun):
        response = mock.Mock(spec=Response)
        response.status_code = 200
        response.json.return_value = {
            "task_instances": [
                {
                    "dag_id": "test_dag",
                    "dag_run_id": "test_run",
                    "execution_date": "2023-01-01T00:00:00.000000+00:00",
                    "task_id": "hello",
                }
            ]
        }
        with patch.object(api_client, "call_api", return_value=response) as mock_call_api:
            endpoint = "/api/v1/dags/{dag_id}/dagRuns/{run_id}/clear".format(
                dag_id="test_dag", run_id="test_run"
            )
            data = {"dry_run": False}
            api_client.clear_dag_run("test_dag", "test_run")
        mock_call_api.assert_called_once_with(endpoint, "POST", json.dumps(data))

    def test_unpause_dag(self, api_client, dagrun):
        response = mock.Mock(spec=Response)
        response.status_code = 200
        response.json.return_value = {
            "dag_id": "test_dag",
            "is_paused": False,
            "is_active": True,
            "is_subdag": False,
        }
        with patch.object(api_client, "call_api", return_value=response) as mock_call_api:
            endpoint = "/api/v1/dags/{dag_id}".format(dag_id="test_dag")
            data = {"is_paused": False}
            api_client.unpause_dag("test_dag")
        mock_call_api.assert_called_once_with(endpoint, "PATCH", json.dumps(data))