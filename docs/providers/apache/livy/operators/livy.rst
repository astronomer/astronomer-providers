Named Hive Partition Sensor Async
"""""""""""""""""""""""""""""""""


To asynchronously submit a Spark application to the underlying cluster asynchronously
:class:`~astronomer.providers.apache.livy.operators.livy.LivyOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/apache/livy/example_dags/example_livy.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_livy_async_with_polling_interval]
    :end-before: [END howto_operator_livy_async_with_polling_interval]
