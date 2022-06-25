GCS Sensor Async
""""""""""""""""


To checks for the existence of a file in Google Cloud Storage
:class:`~astronomer.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_gcs_object_exists_async]
    :end-before: [END howto_sensor_gcs_object_exists_async]


To checks for the existence of GCS objects at a given prefix
:class:`~astronomer.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_gcs_object_with_prefix_existence_async]
    :end-before: [END howto_sensor_gcs_object_with_prefix_existence_async]


To checks for changes in the number of objects at prefix in Google Cloud Storage bucket
:class:`~astronomer.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_gcs_upload_session_complete_async]
    :end-before: [END howto_sensor_gcs_upload_session_complete_async]


To check if an object is updated in Google Cloud Storage
:class:`~astronomer.providers.google.cloud.sensors.gcs.GCSObjectUpdateSensorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_gcs_object_update_async]
    :end-before: [END howto_sensor_gcs_object_update_async]
