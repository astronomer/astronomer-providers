from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)
from astronomer.providers.databricks.triggers.databricks import DatabricksTrigger
from tests.utils.airflow_util import create_context

JOB_ID = "42"
TASK_ID = "databricks_check"
CONN_ID = "databricks_default"
RUN_ID = "1"
RUN_PAGE_URL = "https://www.test.com"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLLING_PERIOD_SECONDS = 1.0
XCOM_RUN_ID_KEY = "run_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"


def make_run_with_state_mock(lifecycle_state: str, result_state: str, state_message: str = ""):
    return {
        "state": {
            "life_cycle_state": lifecycle_state,
            "result_state": result_state,
            "state_message": state_message,
        },
    }


class TestDatabricksSubmitRunOperatorAsync:
    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksSubmitRunOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_job_id")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_submit_run_operator_async_succeeded_before_defered(
        self, submit_run_response, get_job_id, get_run_page_url_response, get_run, defer
    ):
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_job_id.return_value = None
        get_run.return_value = make_run_with_state_mock("TERMINATED", "SUCCESS")

        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )

        operator.execute(context=create_context(operator))

        assert not defer.called

    @pytest.mark.parametrize("result_state", ("FAILED", "UNEXPECTED"))
    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksSubmitRunOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_job_id")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_submit_run_operator_async_failed_before_defered(
        self, submit_run_response, get_job_id, get_run_page_url_response, get_run, defer, result_state
    ):
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_job_id.return_value = None
        get_run.return_value = make_run_with_state_mock("TERMINATED", result_state)

        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )
        with pytest.raises(AirflowException):
            operator.execute(context=create_context(operator))

        assert not defer.called

    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksSubmitRunOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_job_id")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_submit_run_operator_async_failed_with_error_in_run_output_before_defered(
        self,
        submit_run_response,
        get_job_id,
        get_run_page_url_response,
        get_run,
        get_run_output,
        defer,
    ):
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_job_id.return_value = None
        get_run.return_value = make_run_with_state_mock("TERMINATED", "FAILED")
        get_run.return_value["tasks"] = [{"state": {"result_state": "FAILED"}, "run_id": RUN_ID}]
        get_run_output.return_value = {"error": "notebook error"}

        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )
        with pytest.raises(AirflowException):
            operator.execute(context=create_context(operator))

        assert not defer.called

    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksSubmitRunOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_job_id")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_submit_run_operator_async_failed_without_error_in_run_output_before_defered(
        self,
        submit_run_response,
        get_job_id,
        get_run_page_url_response,
        get_run,
        get_run_output,
        defer,
    ):
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_job_id.return_value = None
        get_run.return_value = make_run_with_state_mock("TERMINATED", "FAILED")
        get_run.return_value["tasks"] = [{"state": {"result_state": "FAILED"}, "run_id": RUN_ID}]

        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )
        with pytest.raises(AirflowException):
            operator.execute(context=create_context(operator))

        assert not defer.called

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_job_id")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_submit_run_operator_async(
        self, submit_run_response, get_job_id, get_run_page_url_response, get_run
    ):
        """
        Asserts that a task is deferred and an DatabricksTrigger will be fired
        when the DatabricksSubmitRunOperatorAsync is executed.
        """
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_job_id.return_value = None
        get_run.return_value = make_run_with_state_mock("RUNNING", "SUCCESS")

        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context=create_context(operator))

        assert isinstance(exc.value.trigger, DatabricksTrigger), "Trigger is not a DatabricksTrigger"

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_submit_run_execute_complete_error(
        self, submit_run_response, get_run_page_url_response
    ):
        """
        Asserts that a task is completed with success status.
        """
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL

        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )

        with pytest.raises(AirflowException):
            operator.execute_complete(context={}, event={"status": "error", "message": "error"})

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_submit_run_execute_complete_success(
        self, submit_run_response, get_run_page_url_response
    ):
        """Asserts that a task is completed with success status."""
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL

        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
            do_xcom_push=True,
        )

        assert (
            operator.execute_complete(
                context=create_context(operator),
                event={
                    "status": "success",
                    "message": "success",
                    "job_id": "12345",
                    "run_id": RUN_ID,
                    "run_page_url": RUN_PAGE_URL,
                },
            )
            is None
        )

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator._get_hook")
    def test_databricks_submit_run_operator_async_hook(self, mock_get_hook):
        """
        Asserts that the hook raises TypeError for apache-airflow-providers-databricks>=3.2.0
        when the DatabricksSubmitRunOperatorAsync is executed.
        """
        mock_get_hook.side_effect = TypeError("test exception")
        operator = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )

        with pytest.raises(TypeError):
            operator.execute(context=create_context(operator))


class TestDatabricksRunNowOperatorAsync:
    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksRunNowOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.run_now")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_run_now_operator_async_succeeded_before_defered(
        self, run_now_response, get_run_page_url_response, get_run, defer
    ):
        run_now_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_run.return_value = make_run_with_state_mock("TERMINATED", "SUCCESS")

        operator = DatabricksRunNowOperatorAsync(
            task_id="run_now",
            databricks_conn_id=CONN_ID,
        )

        operator.execute(context=create_context(operator))

        assert not defer.called

    @pytest.mark.parametrize("result_state", ("FAILED", "UNEXPECTED"))
    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksRunNowOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.run_now")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_run_now_operator_async_failed_before_defered(
        self, run_now_response, get_run_page_url_response, get_run, defer, result_state
    ):
        run_now_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_run.return_value = make_run_with_state_mock("TERMINATED", result_state)

        operator = DatabricksRunNowOperatorAsync(
            task_id="run_now",
            databricks_conn_id=CONN_ID,
        )
        with pytest.raises(AirflowException):
            operator.execute(context=create_context(operator))

        assert not defer.called

    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksRunNowOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.run_now")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_run_now_operator_failed_with_error_in_run_output_before_defered(
        self,
        run_now_response,
        get_run_page_url_response,
        get_run,
        get_run_output,
        defer,
    ):
        run_now_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_run.return_value = make_run_with_state_mock("TERMINATED", "FAILED")
        get_run.return_value["tasks"] = [{"state": {"result_state": "FAILED"}, "run_id": RUN_ID}]
        get_run_output.return_value = {"error": "notebook error"}

        operator = DatabricksRunNowOperatorAsync(
            task_id="run_now",
            databricks_conn_id=CONN_ID,
        )
        with pytest.raises(AirflowException):
            operator.execute(context=create_context(operator))

        assert not defer.called

    @mock.patch("astronomer.providers.databricks.operators.databricks.DatabricksRunNowOperatorAsync.defer")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_output")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.run_now")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_run_now_operator_async_failed_without_error_in_run_output_before_defered(
        self,
        run_now_response,
        get_run_page_url_response,
        get_run,
        get_run_output,
        defer,
    ):
        run_now_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_run.return_value = make_run_with_state_mock("TERMINATED", "FAILED")
        get_run.return_value["tasks"] = [{"state": {"result_state": "FAILED"}, "run_id": RUN_ID}]

        operator = DatabricksRunNowOperatorAsync(
            task_id="run_now",
            databricks_conn_id=CONN_ID,
        )
        with pytest.raises(AirflowException):
            operator.execute(context=create_context(operator))

        assert not defer.called

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.run_now")
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_run_now_operator_async(self, run_now_response, get_run_page_url_response, get_run):
        """
        Asserts that a task is deferred and an DatabricksTrigger will be fired
        when the DatabricksRunNowOperatorAsync is executed.
        """
        run_now_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL
        get_run.return_value = make_run_with_state_mock("RUNNING", "SUCCESS")

        operator = DatabricksRunNowOperatorAsync(
            task_id="run_now",
            databricks_conn_id=CONN_ID,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context=create_context(operator))

        assert isinstance(exc.value.trigger, DatabricksTrigger), "Trigger is not a DatabricksTrigger"

    def test_databricks_run_now_execute_complete(self):
        """Asserts that logging occurs as expected"""
        operator = DatabricksRunNowOperatorAsync(
            task_id=TASK_ID,
            databricks_conn_id=CONN_ID,
            do_xcom_push=True,
        )
        operator.run_page_url = RUN_PAGE_URL
        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(create_context(operator), {"status": "success", "message": "success"})
        mock_log_info.assert_called_with("%s completed successfully.", "databricks_check")

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_run_now_execute_complete_error(self, submit_run_response, get_run_page_url_response):
        """Asserts that a task is completed with success status."""
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL

        operator = DatabricksRunNowOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            job_id="12345",
        )

        with pytest.raises(AirflowException):
            operator.execute_complete(context={}, event={"status": "error", "message": "error"})

    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
    @mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
    def test_databricks_run_now_execute_complete_success(
        self, submit_run_response, get_run_page_url_response
    ):
        """Asserts that a task is completed with success status."""
        submit_run_response.return_value = {"run_id": RUN_ID}
        get_run_page_url_response.return_value = RUN_PAGE_URL

        operator = DatabricksRunNowOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            job_id="12345",
            do_xcom_push=True,
        )

        assert (
            operator.execute_complete(
                context=create_context(operator),
                event={
                    "status": "success",
                    "message": "success",
                    "job_id": "12345",
                    "run_id": RUN_ID,
                    "run_page_url": RUN_PAGE_URL,
                },
            )
            is None
        )

    @mock.patch("airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator._get_hook")
    def test_databricks_run_now_operator_async_hook(self, mock_get_hook):
        """
        Asserts that the hook raises TypeError for apache-airflow-providers-databricks>=3.2.0
        when the DatabricksRunNowOperatorAsync is executed.
        """
        mock_get_hook.side_effect = TypeError("test exception")
        operator = DatabricksRunNowOperatorAsync(
            task_id="run_now",
            databricks_conn_id=CONN_ID,
        )

        with pytest.raises(TypeError):
            operator.execute(context=create_context(operator))
