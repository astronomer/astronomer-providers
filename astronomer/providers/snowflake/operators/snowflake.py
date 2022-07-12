import typing
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.context import Context

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync
from astronomer.providers.snowflake.hooks.snowflake_sql_api import (
    SnowflakeSqlApiHookAsync,
)
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSqlApiTrigger,
    SnowflakeTrigger,
    get_db_hook,
)


class SnowflakeOperatorAsync(SnowflakeOperator):
    """
    Executes SQL code in a Snowflake database

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
    """

    def __init__(self, *, poll_interval: int = 5, **kwargs: Any) -> None:
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

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
            self.parameters.get("query_tag", default_query_tag) if self.parameters else default_query_tag
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
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)
        self.query_ids = hook.query_ids

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeTrigger(
                task_id=self.task_id,
                polling_period_seconds=self.poll_interval,
                query_ids=self.query_ids,
                snowflake_conn_id=self.snowflake_conn_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Dict[str, Any], event: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> None:
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
                hook.check_query_output(qids)
                self.log.info("%s completed successfully.", self.task_id)
                return None
        else:
            self.log.info("%s completed successfully.", self.task_id)
            return None


class SnowflakeSqlApiOperatorAsync(SnowflakeOperator):
    """
    Implemented Async Snowflake SQL API Operator to support multiple SQL statements sequentially,
    which is the behavior of the SnowflakeOperator, the Snowflake SQL API allows submitting
    multiple SQL statements in a single request. In combination with aiohttp, make post request to submit SQL
    statements for execution, poll to check the status of the execution of a statement. Fetch query results
    concurrently.
    This Operator currently uses key pair authentication, so you need tp provide private key raw content or
    private key file path in the snowflake connection along with other details

    .. see also::
        https://docs.snowflake.com/en/developer-guide/sql-api/authenticating.html#label-sql-api-authenticating-key-pair

    where can this operator fit in?
         - To Execute Multiple SQL statement in single request
         - To Execute the SQL statement asynchronously and to execute standard queries and most DDL and DML statements
         - To develop custom applications and integrations that perform queries
         - To create provision users and roles, create table, etc.

    The following commands are not supported:
        - The PUT command (in Snowflake SQL)
        - The GET command (in Snowflake SQL)
        - The CALL command with stored procedures that return a table(stored procedures with the RETURNS TABLE clause).

    .. see also::
        To know more about the Snowflake SQL API.
        - https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#introduction-to-the-sql-api
        - https://docs.snowflake.com/en/developer-guide/sql-api/reference.html#snowflake-sql-api-reference
        - https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#limitations-of-the-sql-api
        Limitation on snowflake SQL API
        - https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#limitations-of-the-sql-api

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
    """

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minutes lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes

    def __init__(
        self,
        *,
        poll_interval: int = 5,
        statement_count: int = 0,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        bindings: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self.poll_interval = poll_interval
        self.statement_count = statement_count
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        self.bindings = bindings
        self.execute_async = False
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
        hook.execute_query(self.sql, statement_count=self.statement_count, bindings=self.bindings)
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
        self, context: Dict[str, Any], event: Optional[Dict[str, Union[str, List[str]]]] = None
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
