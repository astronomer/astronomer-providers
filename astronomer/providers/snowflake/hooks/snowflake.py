from __future__ import annotations

import asyncio
from contextlib import closing, contextmanager
from io import StringIO
from typing import Any, Callable, Generator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from asgiref.sync import sync_to_async

from snowflake.connector import DictCursor, ProgrammingError
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements


def fetch_all_snowflake_handler(
    cursor: SnowflakeCursor,
) -> list[tuple[Any, ...]] | list[dict[Any, Any]]:
    """Handler for SnowflakeCursor to return results"""
    return cursor.fetchall()


def fetch_one_snowflake_handler(cursor: SnowflakeCursor) -> dict[str, Any] | tuple[Any, ...] | None:
    """Handler for SnowflakeCursor to return results"""
    return cursor.fetchone()


ABORTING_MESSAGE = "The query is in the process of being aborted on the server side."
FAILED_WITH_ERROR_MESSAGE = "The query finished unsuccessfully."


class SnowflakeHookAsync(SnowflakeHook):
    """
    A client to interact with Snowflake.

    This hook requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation. If used with the S3ToSnowflakeOperator
    add 'aws_access_key_id' and 'aws_secret_access_key' to extra field in the connection.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param warehouse: name of snowflake warehouse
    :param database: name of snowflake database
    :param region: name of snowflake region
    :param role: name of snowflake role
    :param schema: name of snowflake schema
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    """

    def run(  # type: ignore[override]
        self,
        sql: str | list[str],
        autocommit: bool = True,
        parameters: dict | None = None,  # type: ignore[type-arg]
        return_dictionaries: bool = False,
    ) -> list[str]:
        """
        Runs a SQL command or a list of SQL commands.

        :param sql: the sql string to be executed with possibly multiple statements,
          or a list of sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to before executing the query.
        :param parameters: The parameters to render the SQL query with.
        """
        self.query_ids = []
        with closing(self.get_conn()) as conn:
            self.log.info("SQL statement to be executed: %s ", sql)
            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]
            if not sql:
                raise ValueError("List of SQL statements is empty")
            self.set_autocommit(conn, autocommit)
            self.log.debug("Executing %d statements against Snowflake DB", len(sql))
            with self._get_cursor(conn, return_dictionaries) as cur:
                for sql_statement in sql:
                    self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                    if parameters:
                        cur.execute_async(sql_statement, parameters)
                    else:
                        cur.execute_async(sql_statement)
                    query_id = cur.sfqid
                    self.log.info("Snowflake query id: %s", query_id)
                    if query_id:
                        self.query_ids.append(query_id)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()
        return self.query_ids

    def check_query_output(
        self,
        query_ids: list[str],
        handler: Callable[[Any], Any] | None = None,
        return_last: bool = True,
        return_dictionaries: bool = False,
    ) -> Any | list[Any] | None:
        """Once the query is finished fetch the result and log it in airflow"""
        with closing(self.get_conn()) as conn:
            self.set_autocommit(conn, True)
            with self._get_cursor(conn, return_dictionaries) as cur:
                results = []
                for query_id in query_ids:
                    cur.get_results_from_sfqid(query_id)
                    if handler is not None:
                        result = handler(cur)
                        results.append(result)
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", query_id)
            conn.commit()
        if handler is None:
            return None
        elif return_last:
            return results[-1]
        else:
            return results

    async def get_query_status(
        self, query_ids: list[str], poll_interval: float
    ) -> dict[str, str | list[str]]:
        """Get the Query status by query ids."""
        try:
            sfqid = []
            async_connection = await sync_to_async(self.get_conn)()
            try:
                with closing(async_connection) as conn:
                    for query_id in query_ids:
                        while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                            await asyncio.sleep(poll_interval)  # pragma: no cover
                        status = conn.get_query_status(query_id)
                        if status == QueryStatus.SUCCESS:
                            self.log.info("The query finished successfully")
                            sfqid.append(query_id)
                        elif status == QueryStatus.ABORTING:
                            return {
                                "status": "error",
                                "message": ABORTING_MESSAGE,
                                "type": "ABORTING",
                                "query_id": query_id,
                            }
                        elif status == QueryStatus.FAILED_WITH_ERROR:
                            return {
                                "status": "error",
                                "message": FAILED_WITH_ERROR_MESSAGE,
                                "type": "FAILED_WITH_ERROR",
                                "query_id": query_id,
                            }
                        else:
                            return {"status": "error", "message": f"Unknown status: {status}"}
                    return {"status": "success", "query_ids": sfqid}
            except ProgrammingError as err:
                error_message = f"Programming Error: {err}"
                return {"status": "error", "message": error_message, "type": "ERROR"}
        except Exception as e:
            self.log.exception("Unexpected error when retrieving query status:")
            return {"status": "error", "message": str(e), "type": "ERROR"}

    @contextmanager
    def _get_cursor(self, conn: Any, return_dictionaries: bool) -> Generator[SnowflakeCursor, None, None]:
        cursor = None
        try:
            if return_dictionaries:
                cursor = conn.cursor(DictCursor)
            else:
                cursor = conn.cursor()
            yield cursor
        finally:
            if cursor is not None:
                cursor.close()
