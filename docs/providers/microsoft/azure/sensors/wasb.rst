WASB Blob Sensor Async
""""""""""""""""""""""


To poll asynchronously for the existence of a blob in a WASB container.
:class:`~astronomer.providers.microsoft.azure.sensors.wasb.WasbBlobSensorAsync`.

.. exampleinclude:: /../astronomer/providers/microsoft/azure/example_dags/example_wasb_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wasb_blob_sensor_async]
    :end-before: [END howto_sensor_wasb_blob_sensor_async]


To poll asynchronously for the existence of a blob having the given prefix in a WASB container
:class:`~astronomer.providers.microsoft.azure.sensors.wasb.WasbPrefixSensorAsync`.

.. exampleinclude:: /../astronomer/providers/microsoft/azure/example_dags/example_wasb_sensors.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_wasb_prefix_sensor_async]
    :end-before: [END howto_sensor_wasb_prefix_sensor_async]
