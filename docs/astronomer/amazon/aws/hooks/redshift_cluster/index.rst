:py:mod:`amazon.aws.hooks.redshift_cluster`
===========================================

.. py:module:: amazon.aws.hooks.redshift_cluster


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.redshift_cluster.RedshiftHookAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.redshift_cluster.log


.. py:data:: log
   

   

.. py:class:: RedshiftHookAsync(*args, **kwargs)

   Bases: :py:obj:`astronomer.providers.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync`

   Interact with AWS Redshift using aiobotocore python library

   .. py:method:: cluster_status(self, cluster_identifier)
      :async:

      Connects to the AWS redshift cluster via aiobotocore and get the status
      and returns the status of the cluster based on the cluster_identifier passed

      :param cluster_identifier: unique identifier of a cluster


   .. py:method:: pause_cluster(self, cluster_identifier)
      :async:

      Connects to the AWS redshift cluster via aiobotocore and
      pause the cluster based on the cluster_identifier passed

      :param cluster_identifier: unique identifier of a cluster


   .. py:method:: resume_cluster(self, cluster_identifier)
      :async:

      Connects to the AWS redshift cluster via aiobotocore and
      resume the cluster for the cluster_identifier passed

      :param cluster_identifier: unique identifier of a cluster


   .. py:method:: get_cluster_status(self, cluster_identifier, expected_state, flag)
      :async:

      Make call self.cluster_status to know the status and run till the expected_state is met and set the flag

      :param cluster_identifier: unique identifier of a cluster
      :param expected_state: expected_state example("available", "pausing", "paused"")
      :param flag: asyncio even flag set true if success and if any error



