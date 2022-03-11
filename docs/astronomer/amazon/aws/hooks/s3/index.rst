:py:mod:`amazon.aws.hooks.s3`
=============================

.. py:module:: amazon.aws.hooks.s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.s3.S3HookAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   amazon.aws.hooks.s3.log


.. py:data:: log
   

   

.. py:class:: S3HookAsync(*args, **kwargs)

   Bases: :py:obj:`astronomer.providers.amazon.aws.hooks.base_aws_async.AwsBaseHookAsync`

   Interact with AWS S3, using the aiobotocore library.

   .. py:attribute:: conn_type
      :annotation: = s3

      

   .. py:attribute:: hook_name
      :annotation: = S3

      

   .. py:method:: list_prefixes(self, client, bucket_name = None, prefix = None, delimiter = None, page_size = None, max_items = None)
      :async:

      Lists prefixes in a bucket under prefix

      :param client: ClientCreatorContext
      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param delimiter: the delimiter marks key hierarchy.
      :param page_size: pagination size
      :param max_items: maximum items to return
      :return: a list of matched prefixes


   .. py:method:: check_key(self, client, bucket, key, wildcard_match)
      :async:

      Checks if key exists or a key matching a wildcard expression exists in a bucket asynchronously

      :param client: aiobotocore client
      :param bucket: the name of the bucket
      :param key: S3 key that will point to the file
      :param wildcard_match: the path to the key
      :return: True if a key exists and False if not.


   .. py:method:: check_for_prefix(self, client, prefix, delimiter, bucket_name = None)
      :async:

      Checks that a prefix exists in a bucket

      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param delimiter: the delimiter marks key hierarchy.
      :return: False if the prefix does not exist in the bucket and True if it does.


   .. py:method:: get_files(self, client, bucket, key, wildcard_match, delimiter = '/')
      :async:

      Gets a list of files in the bucket


   .. py:method:: is_keys_unchanged(self, client, bucket_name, prefix, inactivity_period = 60 * 60, min_objects = 1, previous_objects = set(), inactivity_seconds = 0, allow_delete = True, last_activity_time = None)
      :async:

      Checks whether new objects have been uploaded and the inactivity_period
      has passed and updates the state of the sensor accordingly.

      :param client: aiobotocore client
      :param bucket_name: the name of the bucket
      :param prefix: a key prefix
      :param inactivity_period:  the total seconds of inactivity to designate
          keys unchanged. Note, this mechanism is not real time and
          this operator may not return until a poke_interval after this period
          has passed with no additional objects sensed.
      :param min_objects: the minimum number of objects needed for keys unchanged
          sensor to be considered valid.
      :param previous_objects: the set of object ids found during the last poke.
      :param inactivity_seconds: number of inactive seconds
      :param last_activity_time: last activity datetime
      :param allow_delete: Should this sensor consider objects being deleted
          between pokes valid behavior. If true a warning message will be logged
          when this happens. If false an error will be raised.
      :return: dictionary with status and message
      :rtype: Dict



