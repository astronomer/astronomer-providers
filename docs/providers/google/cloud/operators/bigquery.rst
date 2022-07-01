Bigquery Operator Async
"""""""""""""""""""""""


To execute BigQuery job asynchronously
:class:`~astronomer.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_insert_job_async]
    :end-before: [END howto_operator_bigquery_insert_job_async]


To performs checks against BigQuery result asynchronously
:class:`~astronomer.providers.google.cloud.operators.bigquery.BigQueryCheckOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_check_async]
    :end-before: [END howto_operator_bigquery_check_async]


To fetches the data from a BigQuery table asynchronously
:class:`~astronomer.providers.google.cloud.operators.bigquery.BigQueryGetDataOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_get_data_async]
    :end-before: [END howto_operator_bigquery_get_data_async]


To checks that the values of metrics given as SQL expressions are within a certain tolerance
:class:`~astronomer.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_interval_check_async]
    :end-before: [END howto_operator_bigquery_interval_check_async]


To performs a simple value check using sql code
:class:`~astronomer.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_value_check_async]
    :end-before: [END howto_operator_bigquery_value_check_async]
