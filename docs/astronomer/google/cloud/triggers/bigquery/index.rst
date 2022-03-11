:py:mod:`google.cloud.triggers.bigquery`
========================================

.. py:module:: google.cloud.triggers.bigquery


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   google.cloud.triggers.bigquery.BigQueryInsertJobTrigger
   google.cloud.triggers.bigquery.BigQueryCheckTrigger
   google.cloud.triggers.bigquery.BigQueryGetDataTrigger
   google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger
   google.cloud.triggers.bigquery.BigQueryValueCheckTrigger




.. py:class:: BigQueryInsertJobTrigger(conn_id, job_id, project_id, dataset_id = None, table_id = None, poll_interval = 4.0)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes BigQueryInsertJobTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Gets current job execution status and yields a TriggerEvent



.. py:class:: BigQueryCheckTrigger(conn_id, job_id, project_id, dataset_id = None, table_id = None, poll_interval = 4.0)

   Bases: :py:obj:`BigQueryInsertJobTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes BigQueryCheckTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Gets current job execution status and yields a TriggerEvent



.. py:class:: BigQueryGetDataTrigger(conn_id, job_id, project_id, dataset_id = None, table_id = None, poll_interval = 4.0)

   Bases: :py:obj:`BigQueryInsertJobTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes BigQueryInsertJobTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Gets current job execution status and yields a TriggerEvent with response data



.. py:class:: BigQueryIntervalCheckTrigger(conn_id, first_job_id, second_job_id, project_id, table, metrics_thresholds, date_filter_column = 'ds', days_back = -7, ratio_formula = 'max_over_min', ignore_zero = True, dataset_id = None, table_id = None, poll_interval = 4.0)

   Bases: :py:obj:`BigQueryInsertJobTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes BigQueryCheckTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Gets current job execution status and yields a TriggerEvent



.. py:class:: BigQueryValueCheckTrigger(conn_id, sql, pass_value, job_id, project_id, tolerance = None, dataset_id = None, table_id = None, poll_interval = 4.0)

   Bases: :py:obj:`BigQueryInsertJobTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes BigQueryValueCheckTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Gets current job execution status and yields a TriggerEvent
