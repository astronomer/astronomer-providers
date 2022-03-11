:py:mod:`google.cloud.sensors.gcs`
==================================

.. py:module:: google.cloud.sensors.gcs

.. autoapi-nested-parse::

   This module contains Google Cloud Storage sensors.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   google.cloud.sensors.gcs.GCSObjectExistenceSensorAsync
   google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensorAsync
   google.cloud.sensors.gcs.GCSUploadSessionCompleteSensorAsync
   google.cloud.sensors.gcs.GCSObjectUpdateSensorAsync




.. py:class:: GCSObjectExistenceSensorAsync(*, bucket, object, polling_interval = 5.0, google_cloud_conn_id = 'google_cloud_default', delegate_to = None, impersonation_chain = None, **kwargs)

   Bases: :py:obj:`airflow.models.baseoperator.BaseOperator`

   Checks for the existence of a file in Google Cloud Storage.

   :param bucket: The Google Cloud Storage bucket where the object is.
   :param object: The name of the object to check in the Google cloud
       storage bucket.
   :param google_cloud_conn_id: The connection ID to use when
       connecting to Google Cloud Storage.
   :param bucket: The bucket name where the objects in GCS will be present
   :param object: the object name of the file or folder present in the google
         cloud storage
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).

   .. py:attribute:: template_fields
      :annotation: = ['bucket', 'object', 'impersonation_chain']



   .. py:attribute:: ui_color
      :annotation: = #f0eee4



   .. py:method:: execute(self, context)

      Airflow runs this method on the worker and defers using the trigger.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: GCSObjectsWithPrefixExistenceSensorAsync(polling_interval = 5.0, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`

   Async Operator that Checks for the existence of GCS objects at a given prefix, passing matches via XCom.

   When files matching the given prefix are found, the poke method's criteria will be
   fulfilled and the matching objects will be returned from the operator and passed
   through XCom for downstream tasks.

   :param bucket: The Google Cloud Storage bucket where the object is.
   :param prefix: The name of the prefix to check in the Google cloud storage bucket.
   :param google_cloud_conn_id: The connection ID to use when connecting to Google Cloud Storage.
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :param polling_interval: The interval in seconds to wait between checks for matching objects.

   .. py:method:: execute(self, context)

      Airflow runs this method on the worker and defers using the trigger.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: GCSUploadSessionCompleteSensorAsync(polling_interval = 5.0, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`

   Checks for changes in the number of objects at prefix in Google Cloud Storage
   bucket and returns True if the inactivity period has passed with no
   increase in the number of objects. Note, this sensor will no behave correctly
   in reschedule mode, as the state of the listed objects in the GCS bucket will
   be lost between rescheduled invocations.

   :param bucket: The Google Cloud Storage bucket where the objects are expected.
   :param prefix: The name of the prefix to check in the Google cloud storage bucket.
   :param inactivity_period: The total seconds of inactivity to designate
       an upload session is over. Note, this mechanism is not real time and
       this operator may not return until a poke_interval after this period
       has passed with no additional objects sensed.
   :param min_objects: The minimum number of objects needed for upload session
       to be considered valid.
   :param previous_objects: The set of object ids found during the last poke.
   :param allow_delete: Should this sensor consider objects being deleted
       between pokes valid behavior. If true a warning message will be logged
       when this happens. If false an error will be raised.
   :param google_cloud_conn_id: The connection ID to use when connecting
       to Google Cloud Storage.
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :param polling_interval: The interval in seconds to wait between checks for matching objects.

   .. py:method:: execute(self, context)

      Airflow runs this method on the worker and defers using the trigger.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: GCSObjectUpdateSensorAsync(polling_interval = 5, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensor`

   Async version to check if an object is updated in Google Cloud Storage

   :param bucket: The Google Cloud Storage bucket where the object is.
   :param object: The name of the object to download in the Google cloud
       storage bucket.
   :param ts_func: Callback for defining the update condition. The default callback
       returns execution_date + schedule_interval. The callback takes the context
       as parameter.
   :param google_cloud_conn_id: The connection ID to use when
       connecting to Google Cloud Storage.
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).

   .. py:method:: execute(self, context)

      Airflow runs this method on the worker and defers using the trigger.


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.
