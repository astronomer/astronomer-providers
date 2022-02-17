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

import unittest
from unittest import mock

import pytest
from airflow.exceptions import TaskDeferred
from airflow.models.dag import DAG
from airflow.utils import timezone

from astronomer_operators.snowflake.operators.snowflake import SnowflakeOperatorAsync
from astronomer_operators.snowflake.triggers.snowflake_trigger import SnowflakeTrigger

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"
LONG_MOCK_PATH = "astronomer_operators.snowflake.operators.snowflake."
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


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


class TestSnowflakeOperator(unittest.TestCase):
    def setUp(self):
        super().setUp()
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    @mock.patch(LONG_MOCK_PATH)
    def test_snowflake_operator(self, mock_get_db_hook):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        operator = SnowflakeOperatorAsync(
            task_id="basic_snowflake", sql=sql, dag=self.dag, do_xcom_push=False
        )
        # do_xcom_push=False because otherwise the XCom test will fail due to the mocking (it actually works)
        # Dummy change
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch(LONG_MOCK_PATH)
    def test_snowflake_execute_operator_async(self, context):
        """
        Asserts that a task is deferred and an SnowflakeTrigger will be fired
        when the SnowflakeOperatorAsync is executed.
        """
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """

        operator = SnowflakeOperatorAsync(
            task_id="execute_run",
            snowflake_conn_id=CONN_ID,
            sql=sql,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)

        assert isinstance(exc.value.trigger, SnowflakeTrigger), "Trigger is not a SnowflakeTrigger"
