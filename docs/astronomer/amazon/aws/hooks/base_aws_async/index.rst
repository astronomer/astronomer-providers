:py:mod:`amazon.aws.hooks.base_aws_async`
=========================================

.. py:module:: amazon.aws.hooks.base_aws_async


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.base_aws_async.AwsBaseHookAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.base_aws_async.log


.. py:data:: log




.. py:class:: AwsBaseHookAsync(aws_conn_id = default_conn_name, verify = None, region_name = None, client_type = None, resource_type = None, config = None)

   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interacts with AWS using aiobotocore asynchronously.

   .. py:method:: get_client_async(self)
      :async:

      Create an Async Client object to communicate with AWS services.
