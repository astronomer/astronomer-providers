import asyncio
from contextlib import closing
from io import StringIO
from typing import Optional, Union

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from asgiref.sync import sync_to_async
from snowflake.connector import DictCursor, ProgrammingError
from snowflake.connector.constants import QueryStatus
from snowflake.connector.util_text import split_statements


class SnowflakeHookAsync(SnowflakeHook):
    def run(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially. The variable query_ids is returned so that
        it can be used to check the  modify the behavior
        depending on the result of the query
        :param sql: the sql string to be executed with possibly multiple statements,
          or a list of sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: dict or iterable
        :param handler: The result handler which is called with the result of each statement.
        :type handler: callable
        """
        self.query_ids = []
        with closing(self.get_conn()) as conn:
            self.set_autocommit(conn, autocommit)

            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            self.log.debug("Executing %d statements against Snowflake DB", len(sql))
            with closing(conn.cursor(DictCursor)) as cur:

                for sql_statement in sql:

                    self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                    if parameters:
                        cur.execute_async(sql_statement, parameters)
                    else:
                        cur.execute_async(sql_statement)
                    self.log.info("Snowflake query id: %s", cur.sfqid)
                    query_id = cur.sfqid
                    self.query_ids.append(query_id)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()
        return self.query_ids

    def check_query_output(self, query_ids):
        """
        Once the qurey is finished fetch the result and log it in airflow
        """
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor(DictCursor)) as cur:
                for query_id in query_ids:
                    cur.get_results_from_sfqid(query_id)
                    results = cur.fetchall()
                    print(f"{results[0]}")
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", query_id)

    async def get_query_status(self, query_ids):
        """
        Async function to get the Query status by query Ids, this function takes list of query_ids make
        sync_to_async connection
        to snowflake to get the query status by query id returns the query status.
        """
        try:
            sfqid = []
            async_connection = await sync_to_async(self.get_conn)()
            try:
                with closing(async_connection) as conn:
                    for query_id in query_ids:
                        while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                            await asyncio.sleep(1)
                        status = conn.get_query_status(query_id)
                        if status == QueryStatus.SUCCESS:
                            self.log.info("The query finished successfully")
                            sfqid.append(query_id)
                        elif status == QueryStatus.ABORTING:
                            return {
                                "status": "error",
                                "message": "The query is in the process of being aborted on the server side.",
                                "type": "ABORTING",
                                "query_id": query_id,
                            }
                        elif status == QueryStatus.FAILED_WITH_ERROR:
                            return {
                                "status": "error",
                                "message": "The query finished unsuccessfully.",
                                "type": "FAILED_WITH_ERROR",
                                "query_id": query_id,
                            }
                        else:
                            return status
                    return {"status": "success", "query_ids": sfqid}
            except ProgrammingError as err:
                error_message = "Programming Error: {0}".format(err)
                self.log.error("error_message ", error_message)
                return {"status": "error", "message": error_message, "type": "ERROR"}
        except Exception as e:
            print(e)
            self.log.error(e)
            return {"status": "error", "message": e, "type": "ERROR"}

    def test_connection(self):
        """Test the Snowflake connection by running a simple query."""
        try:
            self.run(sql="select 1")
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"
