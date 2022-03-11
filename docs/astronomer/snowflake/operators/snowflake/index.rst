:py:mod:`snowflake.operators.snowflake`
=======================================

.. py:module:: snowflake.operators.snowflake


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   snowflake.operators.snowflake.SnowflakeOperatorAsync




.. py:class:: SnowflakeOperatorAsync(*, poll_interval = 5, **kwargs)

   Bases: :py:obj:`airflow.providers.snowflake.operators.snowflake.SnowflakeOperator`

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

   .. py:method:: get_db_hook(self)

      Get the Snowflake Hook


   .. py:method:: execute(self, context)

      Make a sync connection to snowflake and run query in execute_async
      function in snowflake and close the connection


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.
