import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dag import DAG
from snowflake.connector.constants import QueryStatus

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync
from astronomer.providers.snowflake.operators.snowflake import (
    SnowflakeOperatorAsync,
    SnowflakeSqlApiOperatorAsync,
    _check_queries_finish,
)
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSqlApiTrigger,
    SnowflakeTrigger,
)
from tests.utils.airflow_util import create_context

MODULE = "astronomer.providers.snowflake"
TASK_ID = "snowflake_check"
CONN_ID = "my_snowflake_conn"
TEST_SQL = "select * from any;"

SQL_MULTIPLE_STMTS = (
    "create or replace table user_test (i int); insert into user_test (i) "
    "values (200); insert into user_test (i) values (300); select i from user_test order by i;"
)

SINGLE_STMT = "select i from user_test order by i;"


def test_check_queries_finish():
    mock_conn = MagicMock()
    mock_conn.get_query_status_throw_if_error.return_value = QueryStatus.SUCCESS
    assert _check_queries_finish(mock_conn, ["test_sfqid_1", "test_sfquid_2"]) is True


class TestSnowflakeOperatorAsync:
    @pytest.mark.parametrize("mock_sql", [TEST_SQL, [TEST_SQL]])
    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.defer")
    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.get_db_hook")
    @mock.patch(f"{MODULE}.operators.snowflake._check_queries_finish")
    def test_snowflake_execute_operator_async_finish_before_deferred(
        self, mock_check, mock_db_hook, mock_defer, mock_sql, caplog
    ):
        """
        Asserts that a task is not finished before it's deferred
        """
        # _check_queries_finish
        dag = DAG("test_snowflake_async_execute_complete_failure", start_date=datetime.datetime(2023, 1, 1))
        operator = SnowflakeOperatorAsync(
            task_id="execute_run",
            snowflake_conn_id=CONN_ID,
            dag=dag,
            sql=mock_sql,
        )
        mock_check.return_value = True

        operator.execute(create_context(operator))
        assert not mock_defer.called

    @pytest.mark.parametrize("mock_sql", [TEST_SQL, [TEST_SQL]])
    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.get_db_hook")
    @mock.patch(f"{MODULE}.operators.snowflake._check_queries_finish")
    def test_snowflake_execute_operator_async_deffered(self, mock_check, mock_db_hook, mock_sql):
        """
        Asserts that a task is deferred and an SnowflakeTrigger will be fired
        when the SnowflakeOperatorAsync is executed.
        """
        mock_check.return_value = False
        dag = DAG("test_snowflake_async_execute_complete_failure", start_date=datetime.datetime(2023, 1, 1))
        operator = SnowflakeOperatorAsync(
            task_id="execute_run",
            snowflake_conn_id=CONN_ID,
            dag=dag,
            sql=mock_sql,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(create_context(operator))

        assert isinstance(exc.value.trigger, SnowflakeTrigger), "Trigger is not a SnowflakeTrigger"

    def test_snowflake_async_execute_complete_failure(self):
        """Tests that an AirflowException is raised in case of error event"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
        )
        with pytest.raises(AirflowException):
            operator.execute_complete(
                context=None,
                event={
                    "status": "error",
                    "message": "Test failure message",
                    "type": "",
                    "query_id": "test_id",
                },
            )

    @pytest.mark.parametrize(
        "mock_event, mock_xcom_push",
        [
            ({"status": "success", "query_ids": ["uuid", "uuid"]}, True),
            ({"status": "success", "query_ids": ["uuid", "uuid"]}, False),
        ],
    )
    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.get_db_hook")
    def test_snowflake_async_execute_complete(self, mock_conn, mock_event, mock_xcom_push):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            do_xcom_push=mock_xcom_push,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("%s completed successfully.", "execute_complete")

    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.get_db_hook")
    def test_snowflake_sql_api_execute_complete_event_none(self, mock_conn):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
        )

        with pytest.raises(AirflowException):
            operator.execute_complete(context=None, event=None)

    def test_get_db_hook(self):
        """Test get_db_hook with async hook"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
        )
        result = operator.get_db_hook()
        assert isinstance(result, SnowflakeHookAsync)


class TestSnowflakeSqlApiOperatorAsync:
    @pytest.mark.parametrize(
        "mock_sql, statement_count, query_ids",
        [(SQL_MULTIPLE_STMTS, 4, (1, 2, 3, 4)), (SINGLE_STMT, 1, (5,))],
    )
    @mock.patch("astronomer.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperatorAsync.defer")
    @mock.patch("requests.session")
    @mock.patch("astronomer.providers.snowflake.operators.snowflake.SnowflakeSqlApiHookAsync")
    def test_snowflake_sql_api_execute_operator_async_succeeded_before_defer(
        self,
        mock_hook,
        mock_session,
        mock_defer,
        mock_sql,
        statement_count,
        query_ids,
    ):
        """
        Asserts that a task is deferred and an SnowflakeSqlApiTrigger will be fired
        when the SnowflakeSqlApiOperatorAsync is executed.
        """
        mock_hook.return_value = MagicMock()
        mock_hook.return_value.query_ids = query_ids
        mock_hook.return_value.get_request_url_header_params.return_value = ("", "", "")

        mock_session.return_value.__enter__.return_value.get.return_value.__enter__.return_value.status_code = (
            200
        )

        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=mock_sql,
            statement_count=statement_count,
        )

        operator.execute(create_context(operator))

        assert not mock_defer.called

    @pytest.mark.parametrize(
        "mock_sql, statement_count, query_ids",
        [(SQL_MULTIPLE_STMTS, 4, (1, 2, 3, 4)), (SINGLE_STMT, 1, (5,))],
    )
    @mock.patch("astronomer.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperatorAsync.defer")
    @mock.patch("requests.session")
    @mock.patch("astronomer.providers.snowflake.operators.snowflake.SnowflakeSqlApiHookAsync")
    def test_snowflake_sql_api_execute_operator_async_failed_before_defer(
        self,
        mock_hook,
        mock_session,
        mock_defer,
        mock_sql,
        statement_count,
        query_ids,
    ):
        """
        Asserts that a task is deferred and an SnowflakeSqlApiTrigger will be fired
        when the SnowflakeSqlApiOperatorAsync is executed.
        """
        mock_hook.return_value = MagicMock()
        mock_hook.return_value.query_ids = query_ids
        mock_hook.return_value.get_request_url_header_params.return_value = ("", "", "")

        mock_session.return_value.__enter__.return_value.get.return_value.__enter__.return_value.status_code = (
            422
        )

        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=mock_sql,
            statement_count=statement_count,
        )

        with pytest.raises(AirflowException):
            operator.execute(create_context(operator))

        assert not mock_defer.called

    @pytest.mark.parametrize(
        "mock_sql, statement_count, query_ids",
        [(SQL_MULTIPLE_STMTS, 4, (1, 2, 3, 4)), (SINGLE_STMT, 1, (5,))],
    )
    @mock.patch("requests.session")
    @mock.patch("astronomer.providers.snowflake.operators.snowflake.SnowflakeSqlApiHookAsync")
    def test_snowflake_sql_api_execute_operator_async(
        self,
        mock_hook,
        mock_session,
        mock_sql,
        statement_count,
        query_ids,
    ):
        """
        Asserts that a task is deferred and an SnowflakeSqlApiTrigger will be fired
        when the SnowflakeSqlApiOperatorAsync is executed.
        """
        mock_hook.return_value = MagicMock()
        mock_hook.return_value.query_ids = query_ids
        mock_hook.return_value.get_request_url_header_params.return_value = ("", "", "")

        mock_session.return_value.__enter__.return_value.get.return_value.__enter__.return_value.status_code = (
            202
        )

        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=mock_sql,
            statement_count=statement_count,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(create_context(operator))

        assert isinstance(
            exc.value.trigger, SnowflakeSqlApiTrigger
        ), "Trigger is not a SnowflakeSqlApiTrigger"

    def test_snowflake_sql_api_execute_complete_failure(self):
        """Test SnowflakeSqlApiOperatorAsync raise AirflowException of error event"""

        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
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
            ({"status": "success", "statement_query_ids": ["uuid", "uuid"]}),
        ],
    )
    @mock.patch(
        "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.check_query_output"
    )
    def test_snowflake_sql_api_execute_complete(self, mock_conn, mock_event):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)
