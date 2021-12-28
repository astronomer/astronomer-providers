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
import select
from contextlib import closing
from copy import deepcopy

import psycopg2
import psycopg2.extras
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extensions import connection

from astronomer_operators.postgres.triggers.postgres import PostgresTrigger


class _PostgresHook(PostgresHook):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.application_name = kwargs.pop("application_name", None)

    @staticmethod
    def wait(conn):
        while True:
            state = conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_WRITE:
                select.select([], [conn.fileno()], [])
            elif state == psycopg2.extensions.POLL_READ:
                select.select([conn.fileno()], [], [])
            else:
                raise psycopg2.OperationalError("poll() returned %s" % state)

    def get_conn(self) -> connection:
        """Establishes a connection to a postgres database."""
        conn_id = getattr(self, self.conn_name_attr)
        conn = deepcopy(self.connection or self.get_connection(conn_id))

        # check for authentication via AWS IAM
        if conn.extra_dejson.get("iam", False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)

        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port,
            application_name=self.application_name,
            async_=True,
        )
        raw_cursor = conn.extra_dejson.get("cursor", False)
        if raw_cursor:
            conn_args["cursor_factory"] = self._get_cursor(raw_cursor)

        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name not in [
                "iam",
                "redshift",
                "cursor",
                "cluster-identifier",
                "aws_conn_id",
            ]:
                conn_args[arg_name] = arg_val

        self.conn = psycopg2.connect(**conn_args)
        return self.conn

    def run(self, sql, autocommit=False, parameters=None, handler=None):
        wait_func = psycopg2.extras.wait_select
        try:
            with closing(self.get_conn()) as conn:
                self.wait(conn)
                with closing(conn.cursor()) as cur:
                    self.log.info("Running statement: %s, parameters: %s", sql, parameters)
                    if parameters:
                        cur.execute(sql, parameters)
                    else:
                        cur.execute(sql)
                    wait_func(conn)
                return conn.get_backend_pid()
        except Exception as e:
            raise AirflowException(e)


class PostgresOperatorAsync(PostgresOperator):
    """
    Executes sql code in a specific Postgres database

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :type postgres_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ("sql",)
    template_fields_renderers = {"sql": "sql"}
    template_ext = (".sql",)
    ui_color = "#ededed"

    def execute(self, context):
        """
        Logic that the operator uses to execute the Postgres trigger,
        and defer execution as expected.
        """
        if not self.autocommit:
            self.log.warning("Autocommit can not be disabled when using PostgresOperatorAsync.")
        if not isinstance(self.sql, str):
            raise AirflowException(
                "PostgresOperatorAsync requires 'sql' to be a string. Passing multiple queries is not supported."
            )
        application_name = f"{self.dag_id}-{self.task_id}"
        self.hook = _PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.database,
            application_name=application_name,
        )
        pid = self.hook.run(self.sql, False, parameters=self.parameters)
        self.defer(
            timeout=self.execution_timeout,
            trigger=PostgresTrigger(
                sql=self.sql,
                postgres_conn_id=self.postgres_conn_id,
                task_id=self.task_id,
                application_name=application_name,
                pid=pid,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed successfully with response %s ",
            self.task_id,
            event["message"],
        )
        return None
