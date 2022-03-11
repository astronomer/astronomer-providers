:py:mod:`google.cloud.triggers.gcs`
===================================

.. py:module:: google.cloud.triggers.gcs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   google.cloud.triggers.gcs.GCSBlobTrigger
   google.cloud.triggers.gcs.GCSPrefixBlobTrigger
   google.cloud.triggers.gcs.GCSUploadSessionTrigger




Attributes
~~~~~~~~~~

.. autoapisummary::

   google.cloud.triggers.gcs.log


.. py:data:: log
   

   

.. py:class:: GCSBlobTrigger(bucket, object_name, polling_period_seconds, google_cloud_conn_id, hook_params)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   A trigger that fires and it finds the requested file or folder present in the given bucket.
   :param bucket: the bucket in the google cloud storage where the objects are residing.
   :param object_name: the file or folder present in the bucket
   :param google_cloud_conn_id: reference to the Google Connection
   :param polling_period_seconds: polling period in seconds to check for file/folder

   .. py:method:: serialize(self)

      Serializes GCSBlobTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Simple loop until the relevant file/folder is found.



.. py:class:: GCSPrefixBlobTrigger(bucket, prefix, polling_period_seconds, google_cloud_conn_id, hook_params)

   Bases: :py:obj:`GCSBlobTrigger`

   A trigger that fires and it looks for all the objects in the given bucket
   which matches the given prefix if not found sleep for certain interval and checks again.

   :param bucket: the bucket in the google cloud storage where the objects are residing.
   :param prefix: The prefix of the blob_names to match in the Google cloud storage bucket
   :param google_cloud_conn_id: reference to the Google Connection
   :param polling_period_seconds: polling period in seconds to check

   .. py:method:: serialize(self)

      Serializes GCSPrefixBlobTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Simple loop until the matches are found for the given prefix on the bucket.



.. py:class:: GCSUploadSessionTrigger(bucket, prefix, polling_period_seconds, google_cloud_conn_id, hook_params, inactivity_period = 60 * 60, min_objects = 1, previous_objects = None, allow_delete = True)

   Bases: :py:obj:`GCSPrefixBlobTrigger`

   Checks for changes in the number of objects at prefix in Google Cloud Storage
   bucket and returns Trigger Event if the inactivity period has passed with no
   increase in the number of objects.

   :param bucket: The Google Cloud Storage bucket where the objects are.
       expected.
   :param prefix: The name of the prefix to check in the Google cloud
       storage bucket.
   :param polling_period_seconds: polling period in seconds to check
   :param inactivity_period: The total seconds of inactivity to designate
       an upload session is over. Note, this mechanism is not real time and
       this operator may not return until a interval after this period
       has passed with no additional objects sensed.
   :param min_objects: The minimum number of objects needed for upload session
       to be considered valid.
   :param previous_objects: The set of object ids found during the last poke.
   :param allow_delete: Should this sensor consider objects being deleted
       between intervals valid behavior. If true a warning message will be logged
       when this happens. If false an error will be raised.
   :param google_cloud_conn_id: The connection ID to use when connecting
       to Google Cloud Storage.

   .. py:method:: serialize(self)

      Serializes GCSUploadSessionTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Simple loop until no change in any new files or deleted in list blob is
      found for the inactivity_period.



