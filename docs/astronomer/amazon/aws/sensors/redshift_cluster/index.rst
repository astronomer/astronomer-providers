:py:mod:`amazon.aws.sensors.redshift_cluster`
=============================================

.. py:module:: amazon.aws.sensors.redshift_cluster


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.sensors.redshift_cluster.RedshiftClusterSensorAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   amazon.aws.sensors.redshift_cluster.log


.. py:data:: log
   

   

.. py:class:: RedshiftClusterSensorAsync(*, poll_interval = 5, **kwargs)

   Bases: :py:obj:`airflow.providers.amazon.aws.sensors.redshift_cluster.RedshiftClusterSensor`

   Waits for a Redshift cluster to reach a specific status.

   :param cluster_identifier: The identifier for the cluster being pinged.    :param target_status: The cluster status desired.

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



