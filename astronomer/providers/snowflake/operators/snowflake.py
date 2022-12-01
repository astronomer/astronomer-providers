from __future__ import annotations

import typing
from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional, Union

from airflow.exceptions import AirflowException

try:
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
except ImportError:  # pragma: no cover
    # For apache-airflow-providers-snowflake > 3.3.0
    # currently added type: ignore[no-redef, attr-defined] and pragma: no cover because this import
    # path won't be available in current setup
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as SnowflakeOperator  # type: ignore[assignment] # noqa: E501 # pragma: no cover

from astronomer.providers.snowflake.hooks.snowflake import (
    SnowflakeHookAsync,
    fetch_all_snowflake_handler,
)
from astronomer.providers.snowflake.hooks.snowflake_sql_api import (
    SnowflakeSqlApiHookAsync,
)
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSqlApiTrigger,
    SnowflakeTrigger,
    get_db_hook,
)
from astronomer.providers.utils.typing_compat import Context


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
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[Dict[str, Any]] = None,
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
        if self.__class__.__base__.__name__ != "SnowflakeOperator":
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
        hook.run(self.sql, parameters=self.parameters)  # type: ignore[arg-type]
        self.query_ids = hook.query_ids

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

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

    def execute_complete(
        self, context: Context, event: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("SQL in execute_complete: %s", self.sql)
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}: {1}".format(event["type"], event["message"])
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


class SnowflakeSqlApiOperatorAsync(SnowflakeOperator):
    """
    Implemented Async Snowflake SQL API Operator to support multiple SQL statements sequentially,
    which is the behavior of the SnowflakeOperator, the Snowflake SQL API allows submitting
    multiple SQL statements in a single request. In combination with aiohttp, make post request to submit SQL
    statements for execution, poll to check the status of the execution of a statement. Fetch query results
    concurrently.
    This Operator currently uses key pair authentication, so you need tp provide private key raw content or
    private key file path in the snowflake connection along with other details

    .. seealso::

        `Snowflake SQL API key pair Authentication <https://docs.snowflake.com/en/developer-guide/sql-api/authenticating.html#label-sql-api-authenticating-key-pair>`_

    Where can this operator fit in?
         - To execute multiple SQL statements in a single request
         - To execute the SQL statement asynchronously and to execute standard queries and most DDL and DML statements
         - To develop custom applications and integrations that perform queries
         - To create provision users and roles, create table, etc.

    The following commands are not supported:
        - The PUT command (in Snowflake SQL)
        - The GET command (in Snowflake SQL)
        - The CALL command with stored procedures that return a table(stored procedures with the RETURNS TABLE clause).

    .. seealso::

        - `Snowflake SQL API <https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#introduction-to-the-sql-api>`_
        - `API Reference <https://docs.snowflake.com/en/developer-guide/sql-api/reference.html#snowflake-sql-api-reference>`_
        - `Limitation on snowflake SQL API <https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#limitations-of-the-sql-api>`_

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
    :param poll_interval: the interval in seconds to poll the query
    :param statement_count: Number of SQL statement to be executed
    :param token_life_time: lifetime of the JWT Token
    :param token_renewal_delta: Renewal time of the JWT Token
    :param bindings: (Optional) Values of bind variables in the SQL statement.
            When executing the statement, Snowflake replaces placeholders (? and :name) in
            the statement with these specified values.
    """  # noqa

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minutes lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[Dict[str, Any]] = None,
        poll_interval: int = 5,
        statement_count: int = 0,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        bindings: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.snowflake_conn_id = snowflake_conn_id
        self.poll_interval = poll_interval
        self.statement_count = statement_count
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        self.bindings = bindings
        self.execute_async = False
        if self.__class__.__base__.__name__ != "SnowflakeOperator":
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
                }
            super().__init__(conn_id=snowflake_conn_id, **kwargs)  # pragma: no cover
        else:
            super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """
        Make a POST API request to snowflake by using SnowflakeSQL and execute the query to get the ids.
        By deferring the SnowflakeSqlApiTrigger class passed along with query ids.
        """
        self.log.info("Executing: %s", self.sql)
        hook = SnowflakeSqlApiHookAsync(
            snowflake_conn_id=self.snowflake_conn_id,
            token_life_time=self.token_life_time,
            token_renewal_delta=self.token_renewal_delta,
        )
        hook.execute_query(
            self.sql, statement_count=self.statement_count, bindings=self.bindings  # type: ignore[arg-type]
        )
        self.query_ids = hook.query_ids
        self.log.info("List of query ids %s", self.query_ids)

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeSqlApiTrigger(
                poll_interval=self.poll_interval,
                query_ids=self.query_ids,
                snowflake_conn_id=self.snowflake_conn_id,
                token_life_time=self.token_life_time,
                token_renewal_delta=self.token_renewal_delta,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Context, event: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}: {1}".format(event["status"], event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                hook = SnowflakeSqlApiHookAsync(snowflake_conn_id=self.snowflake_conn_id)
                query_ids = typing.cast(List[str], event["statement_query_ids"])
                hook.check_query_output(query_ids)
                self.log.info("%s completed successfully.", self.task_id)
        else:
            self.log.info("%s completed successfully.", self.task_id)
