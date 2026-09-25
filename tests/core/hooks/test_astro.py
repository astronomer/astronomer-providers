from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import aiohttp
import pytest
import requests
from aiohttp import ClientResponse, ClientSession
from airflow.exceptions import AirflowException
from airflow.models import Connection

from astronomer.providers.core.hooks.astro import AstroHook
from astronomer.providers.utils.compat import BaseHook

BASE_URL = "https://test.com"
DAG_RUN_ID = "manual__2024-02-14T19:06:32.053905+00:00"
QUOTED_DAG_RUN_ID = "manual__2024-02-14T19%3A06%3A32.053905%2B00%3A00"


@pytest.fixture
def hook():
    with patch.object(AstroHook, "get_conn", autospec=True, return_value=(BASE_URL, "token")):
        yield AstroHook()


def make_response(status=200, payload=None):
    response = MagicMock(spec=requests.Response)
    response.status_code = status
    response.json.return_value = payload
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(str(status))
    return response


def make_async_response(status=200, payload=None):
    response = MagicMock(spec=ClientResponse)
    response.status = status
    response.json.return_value = payload
    return response


@asynccontextmanager
async def yield_response(response):
    yield response


def mock_client_session(mock_session_cls, *responses):
    """Make ``async with ClientSession() as s, s.get(url) as r`` yield ``responses`` in order."""
    session = MagicMock(spec=ClientSession)
    session.get.side_effect = [yield_response(response) for response in responses]
    mock_session_cls.return_value.__aenter__.return_value = session
    return session


class TestAstroHook:
    def test_get_ui_field_behaviour(self):
        hook = AstroHook()

        result = hook.get_ui_field_behaviour()

        expected_result = {
            "hidden_fields": ["login", "port", "schema", "extra"],
            "relabeling": {
                "password": "Astro Cloud API Token",
            },
            "placeholders": {
                "host": "https://clmkpsyfc010391acjie00t1l.astronomer.run/d5lc9c9x",
                "password": "Astro API JWT Token",
            },
        }

        assert result == expected_result

    @pytest.mark.parametrize(
        ("host", "env", "expected_base_url"),
        [
            ("http://conn-host", {"AIRFLOW__API__BASE_URL": "http://api"}, "http://conn-host"),
            (None, {"AIRFLOW__API__BASE_URL": "http://api"}, "http://api"),
            (None, {"AIRFLOW__WEBSERVER__BASE_URL": "http://webserver"}, "http://webserver"),
            (
                None,
                {"AIRFLOW__API__BASE_URL": "http://api", "AIRFLOW__WEBSERVER__BASE_URL": "http://webserver"},
                "http://api",
            ),
        ],
    )
    def test_get_conn_base_url(self, monkeypatch, host, env, expected_base_url):
        monkeypatch.delenv("AIRFLOW__API__BASE_URL", raising=False)
        monkeypatch.delenv("AIRFLOW__WEBSERVER__BASE_URL", raising=False)
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        conn = Connection(conn_id="astro", conn_type="http", host=host, password="token")

        with patch.object(
            BaseHook, "get_connection", autospec=True, return_value=conn
        ) as mock_get_connection:
            result = AstroHook("astro").get_conn()

        assert result == (expected_base_url, "token")
        mock_get_connection.assert_called_once_with("astro")

    @pytest.mark.parametrize(
        ("host", "password"),
        [(None, "token"), ("http://conn-host", None)],
        ids=["missing-host", "missing-token"],
    )
    def test_get_conn_missing_field(self, monkeypatch, host, password):
        monkeypatch.delenv("AIRFLOW__API__BASE_URL", raising=False)
        monkeypatch.delenv("AIRFLOW__WEBSERVER__BASE_URL", raising=False)
        conn = Connection(conn_id="astro", conn_type="http", host=host, password=password)

        with (
            patch.object(BaseHook, "get_connection", autospec=True, return_value=conn),
            pytest.raises(AirflowException),
        ):
            AstroHook("astro").get_conn()

    def test_headers(self, hook):
        assert hook._headers == {"accept": "application/json", "Authorization": "Bearer token"}

    @pytest.mark.parametrize(("status", "expected_version"), [(200, "v2"), (404, "v1")])
    @patch("astronomer.providers.core.hooks.astro.requests.get", autospec=True)
    def test_get_api_version_probes_once(self, mock_requests_get, hook, status, expected_version):
        mock_requests_get.return_value = make_response(status)

        first = hook._get_api_version(BASE_URL, hook._headers)
        second = hook._get_api_version(BASE_URL, hook._headers)

        assert first == second == expected_version
        mock_requests_get.assert_called_once_with(f"{BASE_URL}/api/v2/version", headers=hook._headers)

    @patch("astronomer.providers.core.hooks.astro.requests.get", autospec=True)
    def test_get_api_version_raises_on_error(self, mock_requests_get, hook):
        mock_requests_get.return_value = make_response(401)

        with pytest.raises(requests.HTTPError):
            hook._get_api_version(BASE_URL, hook._headers)

        assert hook._api_version is None

    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("get_dag_runs", ("my_dag",)),
            ("get_dag_run", ("my_dag", DAG_RUN_ID)),
            ("get_task_instance", ("my_dag", DAG_RUN_ID, "my_task")),
        ],
    )
    @patch("astronomer.providers.core.hooks.astro.requests.get", autospec=True)
    def test_sync_methods_look_up_connection_once(self, mock_requests_get, hook, method, args):
        mock_requests_get.side_effect = [make_response(404), make_response(payload={"dag_runs": []})]

        getattr(hook, method)(*args)

        AstroHook.get_conn.assert_called_once_with(hook)

    @pytest.mark.parametrize(("api_version", "order_by"), [("v1", "-execution_date"), ("v2", "-run_after")])
    @patch("astronomer.providers.core.hooks.astro.requests.get", autospec=True)
    def test_get_dag_runs(self, mock_requests_get, hook, api_version, order_by):
        hook._api_version = api_version
        mock_requests_get.return_value = make_response(
            payload={"dag_runs": [{"dag_run_id": "123", "state": "running"}]}
        )

        result = hook.get_dag_runs("my_dag")

        assert result == [{"dag_run_id": "123", "state": "running"}]
        mock_requests_get.assert_called_once_with(
            f"{BASE_URL}/api/{api_version}/dags/my_dag/dagRuns",
            headers=hook._headers,
            params={"limit": 1, "state": ["running", "queued"], "order_by": order_by},
        )

    @patch("astronomer.providers.core.hooks.astro.requests.get", autospec=True)
    def test_get_dag_runs_probes_api_version(self, mock_requests_get, hook):
        mock_requests_get.side_effect = [make_response(404), make_response(payload={"dag_runs": []})]

        result = hook.get_dag_runs("my_dag")

        assert result == []
        assert [c.args[0] for c in mock_requests_get.call_args_list] == [
            f"{BASE_URL}/api/v2/version",
            f"{BASE_URL}/api/v1/dags/my_dag/dagRuns",
        ]

    @pytest.mark.parametrize("api_version", ["v1", "v2"])
    @patch("astronomer.providers.core.hooks.astro.requests.get", autospec=True)
    def test_get_dag_run(self, mock_requests_get, hook, api_version):
        hook._api_version = api_version
        mock_requests_get.return_value = make_response(payload={"dag_run_id": DAG_RUN_ID, "state": "running"})

        result = hook.get_dag_run("my_dag", DAG_RUN_ID)

        assert result == {"dag_run_id": DAG_RUN_ID, "state": "running"}
        mock_requests_get.assert_called_once_with(
            f"{BASE_URL}/api/{api_version}/dags/my_dag/dagRuns/{QUOTED_DAG_RUN_ID}", headers=hook._headers
        )

    @pytest.mark.parametrize("api_version", ["v1", "v2"])
    @patch("astronomer.providers.core.hooks.astro.requests.get", autospec=True)
    def test_get_task_instance(self, mock_requests_get, hook, api_version):
        hook._api_version = api_version
        mock_requests_get.return_value = make_response(payload={"task_id": "my_task", "state": "success"})

        result = hook.get_task_instance("my_dag", DAG_RUN_ID, "my_task")

        assert result == {"task_id": "my_task", "state": "success"}
        mock_requests_get.assert_called_once_with(
            f"{BASE_URL}/api/{api_version}/dags/my_dag/dagRuns/{QUOTED_DAG_RUN_ID}/taskInstances/my_task",
            headers=hook._headers,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("status", "expected_version"), [(200, "v2"), (404, "v1")])
    async def test_get_a_dag_run(self, hook, status, expected_version):
        response_data = {"dag_id": "my_dag", "dag_run_id": DAG_RUN_ID, "state": "success"}

        with patch("astronomer.providers.core.hooks.astro.ClientSession", autospec=True) as mock_session_cls:
            session = mock_client_session(
                mock_session_cls, make_async_response(status), make_async_response(payload=response_data)
            )

            result = await hook.get_a_dag_run("my_dag", DAG_RUN_ID)

        assert result == response_data
        assert hook._api_version == expected_version
        assert [c.args[0] for c in session.get.call_args_list] == [
            f"{BASE_URL}/api/v2/version",
            f"{BASE_URL}/api/{expected_version}/dags/my_dag/dagRuns/{QUOTED_DAG_RUN_ID}",
        ]

    @pytest.mark.asyncio
    async def test_get_a_dag_run_raises_when_probe_fails(self, hook):
        probe = make_async_response(401)
        probe.raise_for_status.side_effect = aiohttp.ClientResponseError(
            request_info=MagicMock(spec=aiohttp.RequestInfo), history=(), status=401
        )

        with patch("astronomer.providers.core.hooks.astro.ClientSession", autospec=True) as mock_session_cls:
            session = mock_client_session(mock_session_cls, probe)

            with pytest.raises(aiohttp.ClientResponseError):
                await hook.get_a_dag_run("my_dag", DAG_RUN_ID)

        assert hook._api_version is None
        session.get.assert_called_once_with(f"{BASE_URL}/api/v2/version")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("api_version", ["v1", "v2"])
    async def test_get_a_task_instance(self, hook, api_version):
        hook._api_version = api_version
        response_data = {"dag_id": "my_dag", "task_id": "my_task", "state": "success"}

        with patch("astronomer.providers.core.hooks.astro.ClientSession", autospec=True) as mock_session_cls:
            session = mock_client_session(mock_session_cls, make_async_response(payload=response_data))

            result = await hook.get_a_task_instance("my_dag", DAG_RUN_ID, "my_task")

        assert result == response_data
        session.get.assert_called_once_with(
            f"{BASE_URL}/api/{api_version}/dags/my_dag/dagRuns/{QUOTED_DAG_RUN_ID}/taskInstances/my_task"
        )
