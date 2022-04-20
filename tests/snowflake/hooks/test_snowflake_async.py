from unittest import mock

import pytest
from snowflake.connector.constants import QueryStatus

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync


class TestPytestSnowflakeHookAsync:
    @pytest.mark.parametrize(
        "sql,expected_sql,expected_query_ids",
        [
            ("select * from table", ["select * from table"], ["uuid"]),
            (
                "select * from table;select * from table2",
                ["select * from table;", "select * from table2"],
                ["uuid1", "uuid2"],
            ),
            (["select * from table;"], ["select * from table;"], ["uuid1"]),
            (
                ["select * from table;", "select * from table2;"],
                ["select * from table;", "select * from table2;"],
                ["uuid1", "uuid2"],
            ),
        ],
    )
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
    def test_run_storing_query_ids(self, mock_conn, sql, expected_sql, expected_query_ids):
        """Test run method and store, return the query ids"""
        hook = SnowflakeHookAsync()
        conn = mock_conn.return_value
        cur = mock.MagicMock(rowcount=0)
        conn.cursor.return_value = cur
        type(cur).sfqid = mock.PropertyMock(side_effect=expected_query_ids)
        mock_params = {"mock_param": "mock_param"}
        hook.run(sql, parameters=mock_params)

        cur.execute_async.assert_has_calls([mock.call(query, mock_params) for query in expected_sql])
        assert hook.query_ids == expected_query_ids
        cur.close.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "query_ids, expected_state, expected_result",
        [
            (["uuid"], QueryStatus.SUCCESS, {"status": "success", "query_ids": ["uuid"]}),
            (
                ["uuid1"],
                QueryStatus.ABORTING,
                {
                    "status": "error",
                    "type": "ABORTING",
                    "message": "The query is in the process of being aborted on the server side.",
                    "query_id": "uuid1",
                },
            ),
            (
                ["uuid1"],
                QueryStatus.FAILED_WITH_ERROR,
                {
                    "status": "error",
                    "type": "FAILED_WITH_ERROR",
                    "message": "The query finished unsuccessfully.",
                    "query_id": "uuid1",
                },
            ),
            (
                ["uuid1"],
                QueryStatus.BLOCKED,
                {
                    "status": "error",
                    "message": "Unknown status: QueryStatus.BLOCKED",
                },
            ),
        ],
    )
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
    async def test_get_query_status(self, mock_conn, query_ids, expected_state, expected_result):
        """Test get_query_status async in run state"""
        hook = SnowflakeHookAsync()
        conn = mock_conn.return_value
        conn.is_still_running.return_value = False
        conn.get_query_status.return_value = expected_state
        result = await hook.get_query_status(query_ids=query_ids)
        assert result == expected_result

    @pytest.mark.asyncio
    @mock.patch(
        "astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn",
        side_effect=Exception("Connection Errors"),
    )
    async def test_get_query_status_error(self, mock_conn):
        """Test get_query_status async with exception"""
        hook = SnowflakeHookAsync()
        conn = mock_conn.return_value
        conn.is_still_running.side_effect = Exception("Test exception")
        result = await hook.get_query_status(query_ids=["uuid1"])
        print(result)
        assert result == {"status": "error", "message": "Connection Errors", "type": "ERROR"}

    @pytest.mark.parametrize(
        "query_ids",
        [
            (["uuid", "uuid1"]),
        ],
    )
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
    def test_check_query_output_query_ids(self, mock_conn, query_ids):
        """Test check_query_output by query id passed as params"""
        hook = SnowflakeHookAsync()
        conn = mock_conn.return_value
        cur = mock.MagicMock(rowcount=0)
        conn.cursor.return_value = cur
        hook.check_query_output(query_ids=query_ids)

        cur.get_results_from_sfqid.assert_has_calls([mock.call(query_id) for query_id in query_ids])
        cur.close.assert_called()

    @pytest.mark.parametrize(
        "sql,expected_sql,expected_query_ids",
        [
            ("select * from table", ["select * from table"], ["uuid"]),
            (
                "select * from table;select * from table2",
                ["select * from table;", "select * from table2"],
                ["uuid1", "uuid2"],
            ),
            (["select * from table;"], ["select * from table;"], ["uuid1"]),
            (
                ["select * from table;", "select * from table2;"],
                ["select * from table;", "select * from table2;"],
                ["uuid1", "uuid2"],
            ),
        ],
    )
    @mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
    def test_run_storing_query_ids_without_params(self, mock_conn, sql, expected_sql, expected_query_ids):
        """Test run method without params and store, return the query ids"""
        hook = SnowflakeHookAsync()
        conn = mock_conn.return_value
        cur = mock.MagicMock(rowcount=0)
        conn.cursor.return_value = cur
        type(cur).sfqid = mock.PropertyMock(side_effect=expected_query_ids)
        hook.run(sql)

        cur.execute_async.assert_has_calls([mock.call(query) for query in expected_sql])
        assert hook.query_ids == expected_query_ids
        cur.close.assert_called()
