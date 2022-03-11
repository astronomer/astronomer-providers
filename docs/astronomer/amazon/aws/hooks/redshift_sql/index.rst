:py:mod:`amazon.aws.hooks.redshift_sql`
=======================================

.. py:module:: amazon.aws.hooks.redshift_sql


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.redshift_sql.log


.. py:data:: log
   

   

.. py:class:: RedshiftSQLHookAsync(*args, **kwargs)

   Bases: :py:obj:`astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook`

   Interact with AWS.
   This class is a thin wrapper around the boto3 python library.

   :param aws_conn_id: The Airflow connection used for AWS credentials.
       If this is None or empty then the default boto3 behaviour is used. If
       running Airflow in a distributed manner and aws_conn_id is None or
       empty, then default boto3 configuration would be used (and must be
       maintained on each worker node).
   :param verify: Whether or not to verify SSL certificates.
       https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
   :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
   :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
   :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
   :param config: Configuration for botocore client.
       (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)

   .. py:method:: get_query_status(self, query_ids)
      :async:

      Async function to get the Query status by query Ids, this function
      takes list of query_ids make async connection
      to redshift data to get the query status by query id returns the query status.

      :param query_ids: list of query ids


   .. py:method:: is_still_running(self, qid)
      :async:

      Async function to whether the query is still running or in
      "PICKED", "STARTED", "SUBMITTED" state and returns True else
      return False



