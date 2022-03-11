:py:mod:`google.cloud.hooks.bigquery`
=====================================

.. py:module:: google.cloud.hooks.bigquery


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   google.cloud.hooks.bigquery.BigQueryHookAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   google.cloud.hooks.bigquery.BigQueryJob


.. py:data:: BigQueryJob
   

   

.. py:class:: BigQueryHookAsync(**kwargs)

   Bases: :py:obj:`astronomer.providers.google.common.hooks.base_google.GoogleBaseHookAsync`

   Abstract base class for hooks, hooks are meant as an interface to
   interact with external systems. MySqlHook, HiveHook, PigHook return
   object that can handle the connection and interaction to specific
   instances of these systems, and expose consistent methods to interact
   with them.

   .. py:attribute:: sync_hook_class
      

      

   .. py:method:: get_job_instance(self, project_id, job_id, session)
      :async:

      Get the specified job resource by job ID and project ID.


   .. py:method:: get_job_status(self, job_id, project_id = None)
      :async:

      Polls for job status asynchronously using gcloud-aio.

      Note that an OSError is raised when Job results are still pending.
      Exception means that Job finished with errors


   .. py:method:: get_job_output(self, job_id, project_id = None)
      :async:

      Get the big query job output for the given job id asynchronously using gcloud-aio.


   .. py:method:: get_records(self, query_results, nocast = True)

      Given the output query response from gcloud aio bigquery, convert the response to records.

      :param query_results: the results from a SQL query
      :param nocast: indicates whether casting to bq data type is required or not


   .. py:method:: value_check(self, sql, pass_value, records, tolerance = None)

      Match a single query resulting row and tolerance with pass_value

      :return: If Match fail, we throw an AirflowException.


   .. py:method:: interval_check(self, row1, row2, metrics_thresholds, ignore_zero, ratio_formula)

      Checks that the values of metrics given as SQL expressions are within a certain tolerance

      :param row1: first resulting row of a query execution job for first SQL query
      :param row2: first resulting row of a query execution job for second SQL query
      :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
          example 'COUNT(*)': 1.5 would require a 50 percent or less difference
          between the current day, and the prior days_back.
      :param ignore_zero: whether we should ignore zero metrics
      :param ratio_formula: which formula to use to compute the ratio between
          the two metrics. Assuming cur is the metric of today and ref is
          the metric to today - days_back.
          max_over_min: computes max(cur, ref) / min(cur, ref)
          relative_diff: computes abs(cur-ref) / ref



