:py:mod:`snowflake.hooks.snowflake`
===================================

.. py:module:: snowflake.hooks.snowflake


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   snowflake.hooks.snowflake.SnowflakeHookAsync




.. py:class:: SnowflakeHookAsync(*args, **kwargs)

   Bases: :py:obj:`airflow.providers.snowflake.hooks.snowflake.SnowflakeHook`

   A client to interact with Snowflake.

   This hook requires the snowflake_conn_id connection. The snowflake host, login,
   and, password field must be setup in the connection. Other inputs can be defined
   in the connection or hook instantiation. If used with the S3ToSnowflakeOperator
   add 'aws_access_key_id' and 'aws_secret_access_key' to extra field in the connection.

   :param snowflake_conn_id: Reference to
       :ref:`Snowflake connection id<howto/connection:snowflake>`
   :type snowflake_conn_id: str
   :param account: snowflake account name
   :type account: Optional[str]
   :param authenticator: authenticator for Snowflake.
       'snowflake' (default) to use the internal Snowflake authenticator
       'externalbrowser' to authenticate using your web browser and
       Okta, ADFS or any other SAML 2.0-compliant identify provider
       (IdP) that has been defined for your account
       'https://<your_okta_account_name>.okta.com' to authenticate
       through native Okta.
   :type authenticator: Optional[str]
   :param warehouse: name of snowflake warehouse
   :type warehouse: Optional[str]
   :param database: name of snowflake database
   :type database: Optional[str]
   :param region: name of snowflake region
   :type region: Optional[str]
   :param role: name of snowflake role
   :type role: Optional[str]
   :param schema: name of snowflake schema
   :type schema: Optional[str]
   :param session_parameters: You can set session-level parameters at
       the time you connect to Snowflake
   :type session_parameters: Optional[dict]
   :param insecure_mode: Turns off OCSP certificate checks.
       For details, see: `How To: Turn Off OCSP Checking in Snowflake Client Drivers - Snowflake Community
       <https://community.snowflake.com/s/article/How-to-turn-off-OCSP-checking-in-Snowflake-client-drivers>`__
   :type insecure_mode: Optional[bool]

   .. note::
       get_sqlalchemy_engine() depends on snowflake-sqlalchemy

   .. seealso::
       For more information on how to use this Snowflake connection, take a look at the guide:
       :ref:`howto/operator:SnowflakeOperator`

   .. py:method:: run(self, sql, autocommit = False, parameters = None)

      Runs a SQL command or a list of SQL commands.

      :param sql: the sql string to be executed with possibly multiple statements,
        or a list of sql statements to execute
      :param autocommit: What to set the connection's autocommit setting to before executing the query.
      :param parameters: The parameters to render the SQL query with.


   .. py:method:: check_query_output(self, query_ids)

      Once the query is finished fetch the result and log it in airflow


   .. py:method:: get_query_status(self, query_ids)
      :async:

      Get the Query status by query ids.
