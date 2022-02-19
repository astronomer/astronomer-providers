from unittest import mock

import pytest
from airflow.exceptions import TaskDeferred

from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)
from astronomer.providers.databricks.triggers.databricks import DatabricksTrigger

TASK_ID = "databricks_check"
CONN_ID = "databricks_default"
RUN_ID = "1"
RUN_PAGE_URL = "https://www.test.com"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLLING_PERIOD_SECONDS = 1.0
XCOM_RUN_ID_KEY = "run_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
def test_databricks_submit_run_operator_async(submit_run_response, get_run_page_url_response, context):
    """
    Asserts that a task is deferred and an DatabricksTrigger will be fired
    when the DatabricksSubmitRunOperatorAsync is executed.
    """
    submit_run_response.return_value = {"run_id": RUN_ID}
    get_run_page_url_response.return_value = RUN_PAGE_URL

    operator = DatabricksSubmitRunOperatorAsync(
        task_id="submit_run",
        databricks_conn_id=CONN_ID,
        existing_cluster_id="xxxx-xxxxxx-xxxxxx",
        notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(exc.value.trigger, DatabricksTrigger), "Trigger is not a DatabricksTrigger"


@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.run_now")
@mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
def test_databricks_run_now_operator_async(
    run_now_response,
    get_run_page_url_response,
):
    """
    Asserts that a task is deferred and an DatabricksTrigger will be fired
    when the DatabricksRunNowOperatorAsync is executed.
    """
    run_now_response.return_value = {"run_id": RUN_ID}
    get_run_page_url_response.return_value = RUN_PAGE_URL

    operator = DatabricksRunNowOperatorAsync(
        task_id="run_now",
        databricks_conn_id=CONN_ID,
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(exc.value.trigger, DatabricksTrigger), "Trigger is not a DatabricksTrigger"


def test_databricks_run_now_execute_complete():
    """Asserts that logging occurs as expected"""
    operator = DatabricksRunNowOperatorAsync(
        task_id=TASK_ID,
        databricks_conn_id=CONN_ID,
    )
    operator.run_page_url = RUN_PAGE_URL
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete({})
    mock_log_info.assert_called_with("%s completed successfully.", "databricks_check")
