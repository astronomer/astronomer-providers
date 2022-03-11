:py:mod:`amazon.aws.hooks.redshift_data`
========================================

.. py:module:: amazon.aws.hooks.redshift_data


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.redshift_data.RedshiftDataHook




.. py:class:: RedshiftDataHook(*args, **kwargs)

   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

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

   .. py:method:: get_conn_params(self)

      Helper method to retrieve connection args


   .. py:method:: execute_query(self, sql, params)

      Runs an SQL statement, which can be data manipulation language (DML)
      or data definition language (DDL)

      :param sql: list of query ids
