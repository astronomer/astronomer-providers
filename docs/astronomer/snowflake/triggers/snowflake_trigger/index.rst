:py:mod:`snowflake.triggers.snowflake_trigger`
==============================================

.. py:module:: snowflake.triggers.snowflake_trigger


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   snowflake.triggers.snowflake_trigger.SnowflakeTrigger



Functions
~~~~~~~~~

.. autoapisummary::

   snowflake.triggers.snowflake_trigger.get_db_hook



.. py:function:: get_db_hook(snowflake_conn_id)

   Create and return SnowflakeHookAsync.
   :return: a SnowflakeHookAsync instance.


.. py:class:: SnowflakeTrigger(task_id, polling_period_seconds, query_ids, snowflake_conn_id)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes SnowflakeTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Makes a series of connections to snowflake to get the status of the query
      by async get_query_status function
