#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from unittest import mock

import pytest
from snowflake.connector.constants import QueryStatus

from astronomer_operators.snowflake.hooks.snowflake import SnowflakeHookAsync


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
    @mock.patch("astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
    def test_run_storing_query_ids(self, mock_conn, sql, expected_sql, expected_query_ids):
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
        ],
    )
    @mock.patch("astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
    async def test_get_query_status(self, mock_conn, query_ids, expected_state, expected_result):
        hook = SnowflakeHookAsync()
        conn = mock_conn.return_value
        conn.is_still_running.return_value = False
        conn.get_query_status.return_value = expected_state
        result = await hook.get_query_status(query_ids=query_ids)
        print("Test case")
        assert result == expected_result
