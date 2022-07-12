Snowflake SQL API Operator Async
"""""""""""""""""""""""""""""""""


The Snowflake SQL API allows submitting multiple SQL statements in a single request.
In combination with aiohttp, make post request to submit SQL statements for execution,
poll to check the status of the execution of a statement. Fetch query results concurrently
:class:`~astronomer.providers.snowflake.operators.snowflake_sql_api.SnowflakeSqlApiOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/snowflake/example_dags/example_snowflake_sql_api.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_snowflake_sql_api_async]
    :end-before: [END howto_operator_snowflake_sql_api_async]
