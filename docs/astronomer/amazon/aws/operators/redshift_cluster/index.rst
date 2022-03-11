:py:mod:`amazon.aws.operators.redshift_cluster`
===============================================

.. py:module:: amazon.aws.operators.redshift_cluster


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.operators.redshift_cluster.RedshiftResumeClusterOperatorAsync
   amazon.aws.operators.redshift_cluster.RedshiftPauseClusterOperatorAsync




.. py:class:: RedshiftResumeClusterOperatorAsync(*, poll_interval = 5, **kwargs)

   Bases: :py:obj:`airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftResumeClusterOperator`

   Resume a paused AWS Redshift Cluster

   :param cluster_identifier: id of the AWS Redshift Cluster
   :param aws_conn_id: aws connection to use

   .. py:method:: execute(self, context)

      Logic that the operator uses to correctly identify which trigger to
      execute, and defer execution as expected.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: RedshiftPauseClusterOperatorAsync(*, poll_interval = 5, **kwargs)

   Bases: :py:obj:`airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftPauseClusterOperator`

   Pause an AWS Redshift Cluster if cluster status is in `available` state

   :param cluster_identifier: id of the AWS Redshift Cluster
   :param aws_conn_id: aws connection to use

   .. py:method:: execute(self, context)

      Logic that the operator uses to correctly identify which trigger to
      execute, and defer execution as expected.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



