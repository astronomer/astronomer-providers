Dataproc Operator Async
"""""""""""""""""""""""


To create a new cluster on Google Cloud Dataproc
:class:`~astronomer.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataproc_create_cluster_async]
    :end-before: [END howto_operator_dataproc_create_cluster_async]


To delete a cluster on Google Cloud Dataproc
:class:`~astronomer.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataproc_delete_cluster_async]
    :end-before: [END howto_operator_dataproc_delete_cluster_async]


To submits a job to a dataproc cluster
:class:`~astronomer.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataproc_submit_pig_job_async]
    :end-before: [END howto_operator_dataproc_submit_pig_job_async]


To updates an existing cluster in a Google cloud platform project
:class:`~astronomer.providers.google.cloud.operators.google.DataprocUpdateClusterOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/google/cloud/example_dags/example_dataproc.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataproc_update_cluster_async]
    :end-before: [END howto_operator_dataproc_update_cluster_async]
