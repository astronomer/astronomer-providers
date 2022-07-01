EMR Sensor Async
""""""""""""""""


To waits asynchronously for job running on EMR container to reach the terminal state
:class:`~astronomer.providers.amazon.aws.sensor.emr.EmrContainerSensorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_emr_eks_containers_job.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_emr_job_container_async]
    :end-before: [END howto_sensor_emr_job_container_async]


To waits asynchronously for job running on EMR container to reach the terminal state
:class:`~astronomer.providers.amazon.aws.sensor.emr.EmrJobFlowSensorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_emr_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_emr_job_flow_async]
    :end-before: [END howto_sensor_emr_job_flow_async]


To waits asynchronously until state of the EMR cluster step reaches any of the target states
:class:`~astronomer.providers.amazon.aws.sensor.emr.EmrStepSensorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_emr_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_emr_step_async]
    :end-before: [END howto_sensor_emr_step_async]
