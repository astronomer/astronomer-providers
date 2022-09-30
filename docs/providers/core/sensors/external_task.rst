External Task  Sensor Async
"""""""""""""""""""""""""""


To waits asynchronously for a external task to complete
:class:`~astronomer.providers.core.sensor.external_task.ExternalTaskSensorAsync`.

.. exampleinclude:: /../astronomer/providers/core/example_dags/example_external_task.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_external_task_async]
    :end-before: [END howto_sensor_external_task_async]

External Deployment Task Sensor Async
""""""""""""""""""""""""""""""""""""""


To check for the deployment state by making a HTTP GET request async
:class:`~astronomer.providers.core.sensor.external_task.ExternalDeploymentTaskSensorAsync`.

.. exampleinclude:: /../astronomer/providers/core/example_dags/example_external_deployment_task_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_external_deployment_task_async]
    :end-before: [END howto_sensor_external_deployment_task_async]
