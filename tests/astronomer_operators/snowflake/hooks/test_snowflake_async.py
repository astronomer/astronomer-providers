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
import re
import unittest
from typing import Dict, Union
from unittest import mock

import pytest
from airflow.models import Connection

from astronomer_operators.snowflake.hooks.snowflake import SnowflakeHookAsync

_PASSWORD = "snowflake42"

BASE_CONNECTION_KWARGS: Dict[str, Union[str, Dict[str, str]]] = {
    "login": "user",
    "password": "pw",
    "schema": "public",
    "extra": {
        "database": "db",
        "account": "airflow",
        "warehouse": "af_wh",
        "region": "af_region",
        "role": "af_role",
    },
}


class TestPytestSnowflakeHookAsync:
    @pytest.mark.parametrize(
        "sql,query_ids",
        [
            ("select * from table", ["uuid", "uuid"]),
            ("select * from table;select * from table2", ["uuid", "uuid", "uuid2", "uuid2"]),
            (["select * from table;"], ["uuid", "uuid"]),
            (["select * from table;", "select * from table2;"], ["uuid", "uuid", "uuid2", "uuid2"]),
        ],
    )
    def test_run_storing_query_ids(self, sql, query_ids):
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_SNOWFLAKE_DEFAULT=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ), unittest.mock.patch("airflow.providers.snowflake.hooks.snowflake.connector") as mock_connector:
            hook = SnowflakeHookAsync()
            conn = mock_connector.connect.return_value
            cur = mock.MagicMock(rowcount=0)
            conn.cursor.return_value = cur
            type(cur).sfqid = mock.PropertyMock(side_effect=query_ids)
            mock_params = {"mock_param": "mock_param"}
            hook.run(sql, parameters=mock_params)

            sql_list = sql if isinstance(sql, list) else re.findall(".*?[;]", sql)
            cur.execute.assert_has_calls([mock.call(query, mock_params) for query in sql_list])
            assert hook.query_ids == query_ids[::2]
            cur.close.assert_called()

    @mock.patch("astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.run")
    def test_connection_success(self, mock_run):
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_SNOWFLAKE_DEFAULT=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ):
            hook = SnowflakeHookAsync()
            mock_run.return_value = [{"1": 1}]
            status, msg = hook.test_connection()
            assert status is True
            assert msg == "Connection successfully tested"
            mock_run.assert_called_once_with(sql="select 1")

    @mock.patch(
        "astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.run",
        side_effect=Exception("Connection Errors"),
    )
    def test_connection_failure(self, mock_run):
        with unittest.mock.patch.dict(
            "os.environ", AIRFLOW_CONN_SNOWFLAKE_DEFAULT=Connection(**BASE_CONNECTION_KWARGS).get_uri()
        ):
            hook = SnowflakeHookAsync()
            status, msg = hook.test_connection()
            assert status is False
            assert msg == "Connection Errors"
            mock_run.assert_called_once_with(sql="select 1")
