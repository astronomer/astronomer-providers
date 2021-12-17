import asyncio
from contextlib import closing
from io import StringIO
from typing import Optional, Union
from snowflake.connector import DictCursor
from snowflake.connector.util_text import split_statements
from snowflake.connector import ProgrammingError
from snowflake.connector.constants import QueryStatus
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class SnowflakeHookAsync(SnowflakeHook):

    def run_async(self, sql: Union[str, list], autocommit: bool = False, parameters: Optional[dict] = None):
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
                    self.query_ids.append(cur.sfqid)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()
        self.log.info("query ids, ", self.query_ids)
        return self.query_ids        
    
    def check_query_output(self, query_ids):
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor(DictCursor)) as cur:
                for query_id in query_ids:
                    cur.get_results_from_sfqid(query_id)
                    results = cur.fetchall()
                    print(f'{results[0]}')
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", query_id)

    async def get_query_status(self, query_ids):
        try:
            sfqid = []
            with closing(self.get_conn()) as conn:
                for query_id in query_ids:
                    while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                        await asyncio.sleep(1)
                    status = conn.get_query_status(query_id)
                    if status == QueryStatus.SUCCESS:
                        self.log.info("The query finished successfully")
                        sfqid.append(query_id)
                    elif status == QueryStatus.ABORTING:
                        return {"status": "error", "message": 
                        "The query is in the process of being aborted on the server side.", "type": "ABORTING", "query_id": query_id}
                    elif status == QueryStatus.FAILED_WITH_ERROR:
                        return {"status": "error", "message":
                        "The query finished unsuccessfully.", "type": "FAILED_WITH_ERROR", "query_id": query_id}
                    else:
                        return status
                return {"status": "success", "query_ids": sfqid}
        except ProgrammingError as err:
            error_message =  'Programming Error: {0}'.format(err)
            self.log.error("error_message ", error_message)
            return {"status": "error", "message": error_message, "type": "ERROR"}
