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
from typing import Any, Iterable, List, Mapping, Optional, Union

import asyncpg
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from asgiref.sync import sync_to_async


class PostgresHookAsync(PostgresHook):
    """
    Interact with Postgres.

    You can specify ssl parameters in the extra field of your connection.
    Refer to the asyncpg documentation for more details on connection parameters.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    For Redshift, also use redshift in the extra connection parameters and
    set it to true. The cluster-identifier is extracted from the beginning of
    the host field, so is optional. It can however be overridden in the extra field.
    extras example: ``{"iam":true, "redshift":true, "cluster-identifier": "my_cluster_id"}``

    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :type postgres_conn_id: str
    """

    conn_name_attr = "postgres_conn_id"
    default_conn_name = "postgres_default"
    conn_type = "postgres"
    hook_name = "Postgres"
    supports_autocommit = True

    def __init__(self, retry_limit: int = 3, retry_delay: float = 1.0, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.connection: Optional[Connection] = kwargs.pop("connection", None)
        self.schema: Optional[str] = kwargs.pop("schema", None)
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def run(
        self,
        sql: Union[str, List[str]],
        parameters: Optional[Union[Mapping, Iterable]] = None,
    ) -> Any:
        """Establishes asynchronous connection to postgres database using asyncpg
        and executes SQL."""
        try:
            conn_id = getattr(self, self.conn_name_attr)
            conn = await sync_to_async(self.get_connection)(conn_id)

            conn_args = self._get_conn_args(conn)

            # Create a connection pool and acquire a connection.
            self.log.debug("Connecting to %s", conn_args["host"])
            async with asyncpg.create_pool(**conn_args) as pool:
                async with pool.acquire() as con:
                    attempt_num = 1
                    self.log.info("Successfully acquired connection!")
                    while True:
                        try:
                            response = await con.execute(sql)
                            self.log.info("Query execution response is %s", response)
                            return {"status": "success", "message": response}
                        except Exception as e:
                            self.log.warning(
                                "[Try %d of %d] Request  %s failed.",
                                attempt_num,
                                self.retry_limit,
                                sql,
                            )
                            if attempt_num == self.retry_limit:
                                self.log.error("Database error: %s", e)
                                # In this case, the user probably made a mistake.
                                # Don't retry.
                                return {"status": "error", "message": e}

                        attempt_num += 1
                        await asyncio.sleep(self.retry_delay)
        except AirflowNotFoundException as e:
            return {"status": "error", "message": e.title}

    async def get_first(self, sql, **kwargs):
        conn_id = getattr(self, self.conn_name_attr)
        conn: Connection = await sync_to_async(self.get_connection)(conn_id)

        conn_args = self._get_conn_args(conn)

        self.log.debug("Connecting to %s", conn_args["host"])
        conn: asyncpg.Connection = await asyncpg.connect(**conn_args)
        result: asyncpg.Record = await conn.fetchrow(sql)
        return result

    def _get_conn_args(self, conn: Connection) -> dict:
        """
        Helper function to get connection arguments.
        """
        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=self.schema or conn.schema,
            port=conn.port,
        )

        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name not in [
                "iam",
                "redshift",
                "cursor",
                "cluster-identifier",
                "aws_conn_id",
            ]:
                conn_args[arg_name] = arg_val

        return conn_args
