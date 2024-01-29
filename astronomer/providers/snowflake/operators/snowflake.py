from __future__ import annotations

import logging
import typing
import warnings
from contextlib import closing
from typing import Any, Callable, List

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator

from astronomer.providers.snowflake.hooks.snowflake import (
    SnowflakeHookAsync,
    fetch_all_snowflake_handler,
)
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeTrigger,
    get_db_hook,
)
from astronomer.providers.utils.typing_compat import Context
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus


def _check_queries_finish(conn: SnowflakeConnection, query_ids: list[str]) -> bool:
    """Check whether snowflake queries finish (in aborting, failed_with_error or success)"""
    with closing(conn) as conn:
        for query_id in query_ids:
            status = conn.get_query_status_throw_if_error(query_id)
            if status == QueryStatus.SUCCESS:
                continue
            elif conn.is_still_running(status):
                return False
    return True


class SnowflakeOperatorAsync(SnowflakeOperator):
    """
    - SnowflakeOperatorAsync uses the snowflake python connector ``execute_async`` method to submit a database command
      for asynchronous execution.
    - Submit multiple queries in parallel without waiting for each query to complete.
    - Accepts list of queries or multiple queries with ‘;’ semicolon separated string and params. It loops through the
      queries and execute them in sequence. Uses execute_async method to run the query
    - Once a query is submitted, it executes the query from one connection and gets the query IDs from the
      response and passes it to the Triggerer and closes the connection (so that the worker slots can be freed up).
    - The trigger gets the list of query IDs as input and polls every few seconds to snowflake and checks
      for the query status based on the query ID from different connection.

    Where can this operator fit in?
         - Execute time taking queries which can be executed in parallel
         - For batch based operation like copy or inserting the data in parallel.

    Best practices:
         - Ensure that you know which queries are dependent upon other queries before you run any queries in parallel.
           Some queries are interdependent and order sensitive, and therefore not suitable for parallelizing.
           For example, obviously an INSERT statement should not start until after the corresponding to CREATE TABLE
           statement has finished.
         - Ensure that you do not run too many queries for the memory that you have available.
           Running multiple queries in parallel typically consumes more memory,
           especially if more than one set of results is stored in memory at the same time.
         - Ensure that transaction control statements (BEGIN, COMMIT, and ROLLBACK) do not execute in parallel
           with other statements.

    .. seealso::
        - `Snowflake Async Python connector <https://docs.snowflake.com/en/user-guide/python-connector-example.html#label-python-connector-querying-data-asynchronous.>`_
        - `Best Practices <https://docs.snowflake.com/en/user-guide/python-connector-example.html#best-practices-for-asynchronous-queries>`_

    :param snowflake_conn_id: Reference to Snowflake connection id
    :param sql: the sql code to be executed. (templated)
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param handler: (optional) the function that will be applied to the cursor (default: fetch_all_handler).
    :param return_last: (optional) if return the result of only last statement (default: True).
    :param poll_interval: the interval in seconds to poll the query
    """  # noqa

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict[str, Any] | None = None,
        poll_interval: int = 5,
        handler: Callable[[Any], Any] = fetch_all_snowflake_handler,
        return_last: bool = True,
        **kwargs: Any,
    ) -> None:
        self.poll_interval = poll_interval
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.snowflake_conn_id = snowflake_conn_id
        if self.__class__.__base__.__name__ != "SnowflakeOperator":  # type: ignore[union-attr]
            # It's better to do str check of the parent class name because currently SnowflakeOperator
            # is deprecated and in future OSS SnowflakeOperator may be removed
            if any(
                [warehouse, database, role, schema, authenticator, session_parameters]
            ):  # pragma: no cover
                hook_params = kwargs.pop("hook_params", {})  # pragma: no cover
                kwargs["hook_params"] = {
                    "warehouse": warehouse,
                    "database": database,
                    "role": role,
                    "schema": schema,
                    "authenticator": authenticator,
                    "session_parameters": session_parameters,
                    **hook_params,
                }  # pragma: no cover
            super().__init__(conn_id=snowflake_conn_id, **kwargs)  # pragma: no cover
        else:
            super().__init__(**kwargs)
        self.handler = handler
        self.return_last = return_last

    def get_db_hook(self) -> SnowflakeHookAsync:
        """Get the Snowflake Hook"""
        return get_db_hook(self.snowflake_conn_id)

    def execute(self, context: Context) -> None:
        """
        Make a sync connection to snowflake and run query in execute_async
        function in snowflake and close the connection and with the query ids, fetch the status of the query.
        By deferring the SnowflakeTrigger class pass along with query ids.
        """
        self.log.info("Executing: %s", self.sql)

        default_query_tag = f"airflow_openlineage_{self.task_id}_{self.dag.dag_id}_{context['ti'].try_number}"
        query_tag = (
            self.parameters.get("query_tag", default_query_tag)
            if isinstance(self.parameters, dict)
            else default_query_tag
        )
        session_query_tag = f"ALTER SESSION SET query_tag = '{query_tag}';"
        if isinstance(self.sql, str):
            self.sql = "\n".join([session_query_tag, self.sql])
        else:
            session_query_list = [session_query_tag]
            session_query_list.extend(self.sql)
            self.sql = session_query_list

        self.log.info("SQL after adding query tag: %s", self.sql)

        hook = self.get_db_hook()
        hook.run(self.sql, parameters=self.parameters)
        self.query_ids = hook.query_ids

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        if not _check_queries_finish(hook.get_conn(), self.query_ids):
            logging.info("Task deferred")
            self.defer(
                timeout=self.execution_timeout,
                trigger=SnowflakeTrigger(
                    task_id=self.task_id,
                    poll_interval=self.poll_interval,
                    query_ids=self.query_ids,
                    snowflake_conn_id=self.snowflake_conn_id,
                ),
                method_name="execute_complete",
            )
        else:
            logging.info("Queries finish before deferred")

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("SQL in execute_complete: %s", self.sql)
        if event:
            if "status" in event and event["status"] == "error":
                msg = f"sfquid: {event['query_id']}, {event['type']}: {event['message']}"
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                hook = self.get_db_hook()
                qids = typing.cast(List[str], event["query_ids"])
                results = hook.check_query_output(qids, self.handler, self.return_last)
                self.log.info("%s completed successfully.", self.task_id)
                if self.do_xcom_push:
                    return results
        else:
            raise AirflowException("Did not receive valid event from the trigerrer")


class SnowflakeSqlApiOperatorAsync(SnowflakeSqlApiOperator):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperator` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        warnings.warn(
            (
                "This class is deprecated. "
                "Use `airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperator` "
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)
