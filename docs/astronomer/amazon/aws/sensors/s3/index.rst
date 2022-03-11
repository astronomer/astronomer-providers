:py:mod:`amazon.aws.sensors.s3`
===============================

.. py:module:: amazon.aws.sensors.s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.sensors.s3.S3KeySensorAsync
   amazon.aws.sensors.s3.S3KeySizeSensorAsync
   amazon.aws.sensors.s3.S3KeysUnchangedSensorAsync
   amazon.aws.sensors.s3.S3PrefixSensorAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   amazon.aws.sensors.s3.log


.. py:data:: log
   

   

.. py:class:: S3KeySensorAsync(*, bucket_key, bucket_name = None, wildcard_match = False, aws_conn_id = 'aws_default', verify = None, **kwargs)

   Bases: :py:obj:`airflow.models.baseoperator.BaseOperator`

   Waits for a key (a file-like instance on S3) to be present in a S3 bucket
   asynchronously. S3 being a key/value it does not support folders. The path
   is just a key a resource.

   :param bucket_key: The key being waited on. Supports full s3:// style url
       or relative path from root level. When it's specified as a full s3://
       url, please leave bucket_name as `None`.
   :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
       is not provided as a full s3:// url.
   :param wildcard_match: whether the bucket_key should be interpreted as a
       Unix wildcard pattern
   :param aws_conn_id: a reference to the s3 connection
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: S3KeySizeSensorAsync(*, check_fn = None, **kwargs)

   Bases: :py:obj:`S3KeySensorAsync`

   Waits for a key (a file-like instance on S3) to be present and be more than
   some size in a S3 bucket asynchronously.
   S3 being a key/value it does not support folders. The path is just a key
   a resource.

   :param bucket_key: The key being waited on. Supports full s3:// style url
       or relative path from root level. When it's specified as a full s3://
       url, please leave bucket_name as `None`.
   :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
       is not provided as a full s3:// url.
   :param wildcard_match: whether the bucket_key should be interpreted as a
       Unix wildcard pattern
   :param aws_conn_id: a reference to the s3 connection
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :param check_fn: Function that receives the list of the S3 objects,
       and returns the boolean:
       - ``True``: a certain criteria is met
       - ``False``: the criteria isn't met
       **Example**: Wait for any S3 object size more than 1 megabyte  ::

           def check_fn(self, data: List) -> bool:
               return any(f.get('Size', 0) > 1048576 for f in data if isinstance(f, dict))

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: S3KeysUnchangedSensorAsync(*, bucket_name, prefix, aws_conn_id = 'aws_default', verify = None, inactivity_period = 60 * 60, min_objects = 1, previous_objects = None, allow_delete = True, **kwargs)

   Bases: :py:obj:`airflow.models.baseoperator.BaseOperator`

   Checks for changes in the number of objects at prefix in AWS S3
   bucket and returns True if the inactivity period has passed with no
   increase in the number of objects. Note, this sensor will not behave correctly
   in reschedule mode, as the state of the listed objects in the S3 bucket will
   be lost between rescheduled invocations.

   :param bucket_name: Name of the S3 bucket
   :param prefix: The prefix being waited on. Relative path from bucket root level.
   :param aws_conn_id: a reference to the s3 connection
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :param inactivity_period: The total seconds of inactivity to designate
       keys unchanged. Note, this mechanism is not real time and
       this operator may not return until a poke_interval after this period
       has passed with no additional objects sensed.
   :param min_objects: The minimum number of objects needed for keys unchanged
       sensor to be considered valid.
   :param previous_objects: The set of object ids found during the last poke.
   :param allow_delete: Should this sensor consider objects being deleted
       between pokes valid behavior. If true a warning message will be logged
       when this happens. If false an error will be raised.

   .. py:attribute:: template_fields
      :annotation: :Sequence[str] = ['bucket_name', 'prefix']

      

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: S3PrefixSensorAsync(*, bucket_name, prefix, delimiter = '/', aws_conn_id = 'aws_default', verify = None, **kwargs)

   Bases: :py:obj:`airflow.models.baseoperator.BaseOperator`

   Async implementation of the S3 Prefix Sensor.
   Gets deferred onto the Trigggerer and pokes
   for a prefix or all prefixes to exist.
   A prefix is the first part of a key,thus enabling
   checking of constructs similar to glob ``airfl*`` or
   SQL LIKE ``'airfl%'``. There is the possibility to precise a delimiter to
   indicate the hierarchy or keys, meaning that the match will stop at that
   delimiter. Current code accepts sane delimiters, i.e. characters that
   are NOT special characters in the Python regex engine.

   :param bucket_name: Name of the S3 bucket
   :param prefix: The prefix being waited on. Relative path from bucket root level.
   :param delimiter: The delimiter intended to show hierarchy.
       Defaults to '/'.
   :param aws_conn_id: a reference to the s3 connection
   :param verify: Whether to verify SSL certificates for S3 connection.
       By default, SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.

   .. py:attribute:: template_fields
      :annotation: :Sequence[str] = ['prefix', 'bucket_name']

      

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



