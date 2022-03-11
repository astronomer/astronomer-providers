:py:mod:`databricks.hooks.databricks`
=====================================

.. py:module:: databricks.hooks.databricks


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   databricks.hooks.databricks.DatabricksHookAsync




.. py:class:: DatabricksHookAsync(databricks_conn_id = default_conn_name, timeout_seconds = 180, retry_limit = 3, retry_delay = 1.0)

   Bases: :py:obj:`airflow.providers.databricks.hooks.databricks.DatabricksHook`

   Interact with Databricks.

   :param databricks_conn_id: Reference to the Databricks connection.
   :type databricks_conn_id: str
   :param timeout_seconds: The amount of time in seconds the requests library
       will wait before timing-out.
   :type timeout_seconds: int
   :param retry_limit: The number of times to retry the connection in case of
       service outages.
   :type retry_limit: int
   :param retry_delay: The number of seconds to wait between retries (it
       might be a floating point number).
   :type retry_delay: float

   .. py:method:: get_run_state_async(self, run_id)
      :async:

      Retrieves run state of the run using an asynchronous api call.
      :param run_id: id of the run
      :return: state of the run
