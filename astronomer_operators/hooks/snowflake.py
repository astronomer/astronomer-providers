import asyncio
from contextlib import aclosing
from io import StringIO
from typing import Optional, Union
from snowflake.connector import DictCursor
from snowflake.connector.util_text import split_statements
from snowflake.connector import ProgrammingError
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class SnowflakeHookAsync(SnowflakeHook):

    def run_async(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = Non):
        self.query_ids = []

        with aclosing(self.get_conn()) as conn:
            self.set_autocommit(conn, autocommit)

            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            self.log.debug("Executing %d statements against Snowflake DB", len(sql))
            with aclosing(conn.cursor(DictCursor)) as cur:

                for sql_statement in sql:

                    self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                    if parameters:
                        cur.execute_async(sql_statement, parameters)
                    else:
                        cur.execute_async(sql_statement)

                    execution_info = []
                    for row in cur:
                        self.log.info("Statement execution info - %s", row)
                        execution_info.append(row)

                    # self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", cur.sfqid)
                    self.query_ids.append(cur.sfqid)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()
    
    def check_query_output(self, query_id):
        with aclosing(self.get_conn()) as conn:
            with aclosing(conn.cursor(DictCursor)) as cur:
                cur.get_results_from_sfqid(query_id)
                results = cur.fetchall()
                print(f'{results[0]}')
                self.log.info("Rows affected: %s", cur.rowcount)
                self.log.info("Snowflake query id: %s", query_id)

    async def get_query_status(self, query_id):
        try:
            with aclosing(self.get_conn()) as conn:
                while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                    await asyncio.sleep(1)
        except ProgrammingError as err:
            print('Programming Error: {0}'.format(err))
