:py:mod:`databricks.triggers.databricks`
========================================

.. py:module:: databricks.triggers.databricks


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   databricks.triggers.databricks.DatabricksTrigger




.. py:class:: DatabricksTrigger(conn_id, task_id, run_id, retry_limit, retry_delay, polling_period_seconds)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes DatabricksTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Makes a series of asynchronous http calls via a Databrick hook. It yields a Trigger if
      response is a 200 and run_state is successful, will retry the call up to the retry limit
      if the error is 'retryable', otherwise it throws an exception.



