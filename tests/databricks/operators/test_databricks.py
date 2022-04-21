from datetime import datetime
from unittest import mock

import pytest
from airflow import DAG
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.types import DagRunType

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
@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_job_id")
@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
def test_databricks_submit_run_operator_async(
    submit_run_response, get_job_id, get_run_page_url_response, context
):
    """
    Asserts that a task is deferred and an DatabricksTrigger will be fired
    when the DatabricksSubmitRunOperatorAsync is executed.
    """
    submit_run_response.return_value = {"run_id": RUN_ID}
    get_run_page_url_response.return_value = RUN_PAGE_URL
    get_job_id.return_value = None

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
        do_xcom_push=True,
    )
    operator.run_page_url = RUN_PAGE_URL
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(create_context(operator), {"status": "success", "message": "success"})
    mock_log_info.assert_called_with("%s completed successfully.", "databricks_check")


@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
def test_databricks_submit_run_execute_complete_error(
    submit_run_response, get_run_page_url_response, context
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
def test_databricks_run_now_execute_complete_error(submit_run_response, get_run_page_url_response, context):
    """
    Asserts that a task is completed with success status.
    """
    submit_run_response.return_value = {"run_id": RUN_ID}
    get_run_page_url_response.return_value = RUN_PAGE_URL

    operator = DatabricksRunNowOperatorAsync(
        task_id="submit_run",
        databricks_conn_id=CONN_ID,
        job_id="12345",
    )

    with pytest.raises(AirflowException):
        operator.execute_complete(context={}, event={"status": "error", "message": "error"})


def create_context(task):
    dag = DAG(dag_id="dag")
    execution_date = datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
def test_databricks_submit_run_execute_complete_success(
    submit_run_response, get_run_page_url_response, context
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


@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.submit_run")
@mock.patch("airflow.providers.databricks.hooks.databricks.DatabricksHook.get_run_page_url")
def test_databricks_run_now_execute_complete_success(submit_run_response, get_run_page_url_response, context):
    """
    Asserts that a task is completed with success status.
    """
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
