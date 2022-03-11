:py:mod:`amazon.aws.triggers.redshift_cluster`
==============================================

.. py:module:: amazon.aws.triggers.redshift_cluster


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger
   amazon.aws.triggers.redshift_cluster.RedshiftClusterSensorTrigger




.. py:class:: RedshiftClusterTrigger(task_id, polling_period_seconds, aws_conn_id, cluster_identifier, operation_type)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes RedshiftClusterTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Make async connection to redshift, based on the operation type call
      the RedshiftHookAsync functions
      if operation_type is 'resume_cluster' it will call the resume_cluster function in RedshiftHookAsync
      if operation_type is 'pause_cluster it will call the pause_cluster function in RedshiftHookAsync



.. py:class:: RedshiftClusterSensorTrigger(task_id, aws_conn_id, cluster_identifier, target_status, polling_period_seconds)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes RedshiftClusterSensorTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Simple async function run until the cluster status match the target status.
