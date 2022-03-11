:py:mod:`snowflake.example_dags.example_snowflake`
==================================================

.. py:module:: snowflake.example_dags.example_snowflake

.. autoapi-nested-parse::

   Example use of SnowflakeAsync related providers.



Module Contents
---------------

.. py:data:: SNOWFLAKE_CONN_ID
   :annotation: = my_snowflake_conn

   

.. py:data:: SNOWFLAKE_SAMPLE_TABLE
   :annotation: = sample_table

   

.. py:data:: CREATE_TABLE_SQL_STRING
   

   

.. py:data:: SQL_INSERT_STATEMENT
   

   

.. py:data:: SQL_LIST
   

   

.. py:data:: SQL_MULTIPLE_STMTS
   

   

.. py:data:: SNOWFLAKE_SLACK_SQL
   

   

.. py:data:: SNOWFLAKE_SLACK_MESSAGE
   :annotation: = Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: text
        :linenos:

        Results in an ASCII table:
        ```{{ results_df | tabulate(tablefmt='pretty', headers='keys') }}```

    .. raw:: html

        </details>

   

.. py:data:: dag
   

   

.. py:data:: snowflake_op_sql_str
   

   

.. py:data:: snowflake_op_with_params
   

   

.. py:data:: snowflake_op_sql_list
   

   

.. py:data:: snowflake_op_sql_multiple_stmts
   

   

