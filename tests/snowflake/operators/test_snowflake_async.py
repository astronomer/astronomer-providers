from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.utils import timezone

from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync
from astronomer.providers.snowflake.triggers.snowflake_trigger import SnowflakeTrigger

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"
LONG_MOCK_PATH = "astronomer.providers.snowflake.operators.snowflake."
LONG_MOCK_PATH += "SnowflakeOperatorAsync.get_db_hook"
TASK_ID = "snowflake_check"
CONN_ID = "my_snowflake_conn"
RUN_ID = "1"
RUN_PAGE_URL = "https://www.test.com"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLLING_PERIOD_SECONDS = 1.0
XCOM_RUN_ID_KEY = "run_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"
TEST_SQL = "select * from any"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch(LONG_MOCK_PATH)
def test_snowflake_execute_operator_async(context):
    """
    Asserts that a task is deferred and an SnowflakeTrigger will be fired
    when the SnowflakeOperatorAsync is executed.
    """

    operator = SnowflakeOperatorAsync(
        task_id="execute_run",
        snowflake_conn_id=CONN_ID,
        sql=TEST_SQL,
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(exc.value.trigger, SnowflakeTrigger), "Trigger is not a SnowflakeTrigger"


def test_snowflake_async_execute_complete_failure():
    """Tests that an AirflowException is raised in case of error event"""

    operator = SnowflakeOperatorAsync(
        task_id="execute_complete",
        snowflake_conn_id=CONN_ID,
        sql=TEST_SQL,
    )
    with pytest.raises(AirflowException):
        operator.execute_complete(
            context=None,
            event={"status": "error", "message": "Test failure message", "type": "FAILED_WITH_ERROR"},
        )


@pytest.mark.parametrize(
    "mock_event",
    [
        None,
        ({"status": "success", "query_ids": ["uuid", "uuid"]}),
    ],
)
@mock.patch(LONG_MOCK_PATH)
def test_snowflake_async_execute_complete(mock_conn, mock_event):
    """Tests execute_complete assert with successful message"""

    operator = SnowflakeOperatorAsync(
        task_id="execute_complete",
        snowflake_conn_id=CONN_ID,
        sql=TEST_SQL,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event=mock_event)
    mock_log_info.assert_called_with("%s completed successfully.", "execute_complete")


@mock.patch(LONG_MOCK_PATH)
def test_get_db_hook(mock_get_db_hook):
    """Test get_db_hook with async hook"""
    operator = SnowflakeOperatorAsync(
        task_id="execute_complete",
        snowflake_conn_id=CONN_ID,
        sql=TEST_SQL,
    )
    operator.get_db_hook()
    mock_get_db_hook.assert_called_once()
