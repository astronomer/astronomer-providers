Databricks Operator Async
""""""""""""""""""""""""""""""""""""


To submits a Spark job run to Databricks
:class:`~astronomer.providers.databricks.operators.databricks.DatabricksSubmitRunOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/databricks/example_dags/example_databricks.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_databricks_submit_run_async]
    :end-before: [END howto_operator_databricks_submit_run_async]


To runs an existing Spark job run on Databricks
:class:`~astronomer.providers.databricks.operators.databricks.DatabricksRunNowOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/databricks/example_dags/example_databricks.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_databricks_run_now_async]
    :end-before: [END howto_operator_databricks_run_now_async]
