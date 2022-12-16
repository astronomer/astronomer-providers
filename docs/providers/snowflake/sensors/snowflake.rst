Snowflake Sensor Async
""""""""""""""""""""""""


Runs a sql statement repeatedly until a criteria is met in a Snowflake database. success or failure criteria are met, or if the first cell returned from the query is not in (0, '0', '', None). Optional success and failure callables are called with the first cell returned from the query as the argument. If success callable is defined the sensor will keep retrying until the criteria is met. If failure callable is defined and the criteria is met the sensor will raise AirflowException. Failure criteria is evaluated before success criteria. A fail_on_empty boolean can also be passed to the sensor in which case it will fail if no rows have been returned.
:class:`~astronomer.providers.snowflake.sensors.snowflake.SnowflakeSensorAsync`.

.. exampleinclude:: /../astronomer/providers/snowflake/example_dags/example_snowflake_sensor.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_snowflake_async]
    :end-before: [END howto_sensor_snowflake_async]
