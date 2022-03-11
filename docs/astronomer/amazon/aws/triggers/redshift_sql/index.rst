:py:mod:`amazon.aws.triggers.redshift_sql`
==========================================

.. py:module:: amazon.aws.triggers.redshift_sql


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.triggers.redshift_sql.RedshiftSQLTrigger




.. py:class:: RedshiftSQLTrigger(task_id, polling_period_seconds, aws_conn_id, query_ids)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes RedshiftSQLTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Make async connection and execute query using the Amazon Redshift Data API.



