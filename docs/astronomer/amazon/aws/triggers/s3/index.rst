:py:mod:`amazon.aws.triggers.s3`
================================

.. py:module:: amazon.aws.triggers.s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.triggers.s3.S3KeyTrigger
   amazon.aws.triggers.s3.S3KeySizeTrigger
   amazon.aws.triggers.s3.S3KeysUnchangedTrigger
   amazon.aws.triggers.s3.S3PrefixTrigger




Attributes
~~~~~~~~~~

.. autoapisummary::

   amazon.aws.triggers.s3.log


.. py:data:: log




.. py:class:: S3KeyTrigger(bucket_name, bucket_key, wildcard_match = False, aws_conn_id = 'aws_default', **hook_params)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serialize S3KeyTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Make an asynchronous connection using S3HookAsync.



.. py:class:: S3KeySizeTrigger(bucket_name, bucket_key, wildcard_match = False, aws_conn_id = 'aws_default', check_fn = None, **hook_params)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serialize S3KeySizeTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Make an asynchronous connection using S3HookAsync.



.. py:class:: S3KeysUnchangedTrigger(bucket_name, prefix, inactivity_period = 60 * 60, min_objects = 1, inactivity_seconds = 0, previous_objects = set(), allow_delete = True, aws_conn_id = 'aws_default', last_activity_time = None, verify = None)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serialize S3KeysUnchangedTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Make an asynchronous connection using S3HookAsync.



.. py:class:: S3PrefixTrigger(*, bucket_name, prefix, delimiter = '/', aws_conn_id = 'aws_default', verify = None, **hook_params)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serialize S3PrefixTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Make an asynchronous connection using S3HookAsync.
