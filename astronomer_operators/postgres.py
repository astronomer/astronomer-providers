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

import asyncio
from typing import Any, Dict, Tuple, Union, Optional, List, Mapping, Iterable
from airflow.exceptions import AirflowException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from astronomer_operators.hooks.postgres import PostgresHookAsync

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
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ('sql',)
    template_fields_renderers = {'sql': 'sql'}
    template_ext = ('.sql',)
    ui_color = '#ededed'

    def execute(self, context):
        """
        Logic that the operator uses to execute the Postgres trigger,
        and defer execution as expected.
        """
        self.defer(
            timeout=self.execution_timeout,
            trigger=PostgresTrigger(
                sql = self.sql,
                postgres_conn_id = self.postgres_conn_id,
                task_id=self.task_id,
                parameters = self.parameters,
                database = self.database,
            ),
            method_name="execute_complete",
        )


    def execute_complete(self, context, event=None):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None
            


class PostgresTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        sql: Union[str, List[str]],
        postgres_conn_id: str = 'postgres_default',
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None
    ):
        self.log.info("Executing PostgresTrigger")
        super().__init__()
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.task_id = task_id
        self.parameters = parameters
        self.database = database

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes PostgresTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.postgres.PostgresTrigger",
            {
                "sql": self.sql,
                "postgres_conn_id": self.postgres_conn_id,
                "task_id": self.task_id,
                "parameters": self.parameters,
                "database": self.database,
            },
        )
    
    async def run(self):
        """
        Makes a series of asynchronous query requests via PostgresHookAsync. It yields a Trigger if
        query is successfully executed, will retry the query up to the retry limit
        if there is an error, otherwise it throws an exception.
        """
        hook = self._get_async_hook()
        while True:
            try:
                await hook.run(
                    sql = self.sql, 
                    parameters=self.parameters)
                yield TriggerEvent(True)
                return
            except AirflowException as exc:
                await asyncio.sleep(self.poll_interval)

    
    def _get_async_hook(self) -> PostgresHookAsync:
        return PostgresHookAsync(
        postgres_conn_id=self.postgres_conn_id, 
        schema=self.database
        )