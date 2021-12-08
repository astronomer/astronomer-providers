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

from typing import Iterable, List, Optional, Tuple, Union, Mapping, Any
from airflow.models.connection import Connection
from copy import deepcopy
import asyncpg
import asyncio
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook


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

    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    conn_type = 'postgres'
    hook_name = 'Postgres'
    supports_autocommit = True

    def __init__(
        self,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        *args, 
        **kwargs
        ) -> None:
        super().__init__(*args, **kwargs)
        self.connection: Optional[Connection] = kwargs.pop("connection", None)
        self.schema: Optional[str] = kwargs.pop("schema", None)
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    def get_iam_token(self, conn: Connection) -> Tuple[str, str, int]:
        """
        Uses AWSHook to retrieve a temporary password to connect to Postgres
        or Redshift. Port is required. If none is provided, default is used for
        each service
        """
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        redshift = conn.extra_dejson.get('redshift', False)
        aws_conn_id = conn.extra_dejson.get('aws_conn_id', 'aws_default')
        aws_hook = AwsBaseHook(aws_conn_id, client_type='rds')
        login = conn.login
        if conn.port is None:
            port = 5439 if redshift else 5432
        else:
            port = conn.port
        if redshift:
            # Pull the custer-identifier from the beginning of the Redshift URL
            # ex. my-cluster.ccdre4hpd39h.us-east-1.redshift.amazonaws.com returns my-cluster
            cluster_identifier = conn.extra_dejson.get('cluster-identifier', conn.host.split('.')[0])
            client = aws_hook.get_client_type('redshift')
            cluster_creds = client.get_cluster_credentials(
                DbUser=conn.login,
                DbName=self.schema or conn.schema,
                ClusterIdentifier=cluster_identifier,
                AutoCreate=False,
            )
            token = cluster_creds['DbPassword']
            login = cluster_creds['DbUser']
        else:
            token = aws_hook.conn.generate_db_auth_token(conn.host, port, conn.login)
        return login, token, port

    async def run(
        self,
        sql: Union[str, List[str]],
        parameters: Optional[Union[Mapping, Iterable]] = None
        )-> Any:
        """Establishes asynchronous connection to postgres database using asyncpg
        and executes SQL."""

        conn_id = getattr(self, self.conn_name_attr)
        conn = deepcopy(self.connection or self.get_connection(conn_id))

        # check for authentication via AWS IAM
        if conn.extra_dejson.get('iam', False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)

        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=self.schema or conn.schema,
            port=conn.port,
        )

        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name not in [
                'iam',
                'redshift',
                'cursor',
                'cluster-identifier',
                'aws_conn_id',
            ]:
                conn_args[arg_name] = arg_val

        # Create a connection pool and acquire a connection.
        self.log.info(f"Connecting to {conn_args['host']}")
        async with asyncpg.create_pool(**conn_args) as pool:
            async with pool.acquire() as con:
                attempt_num = 1
                self.log.info("Successfully acquired connection!")
                while True: 
                    try:
                        response = await con.execute(sql)
                        self.log.info(f"Query execution response is {response}")
                        return response
                    except Exception as e:
                        self.log.warning(
                        "[Try %d of %d] Request  %s failed.",
                        attempt_num,
                        self.retry_limit,
                        sql)
                        if (attempt_num == self.retry_limit):
                            self.log.error("Database error: %s", e)
                            # In this case, the user probably made a mistake.
                            # Don't retry.
                            raise AirflowException(str(e.status) + ":" + e.message)

                    attempt_num += 1
                    await asyncio.sleep(self.retry_delay)