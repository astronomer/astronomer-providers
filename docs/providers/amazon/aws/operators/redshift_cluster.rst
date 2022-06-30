Redshift Operator Async
"""""""""""""""""""""""


To delete an AWS Redshift cluster if cluster status is in ``available`` state
:class:`~astronomer.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_delete_cluster_async]
    :end-before: [END howto_operator_redshift_delete_cluster_async]


To pause an AWS Redshift cluster if cluster status is in ``available`` state
:class:`~astronomer.providers.amazon.aws.operators.redshift_cluster.RedshiftPauseClusterOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_pause_cluster_async]
    :end-before: [END howto_operator_redshift_pause_cluster_async]


To resume a paused AWS Redshift cluster asynchronously
:class:`~astronomer.providers.amazon.aws.operators.redshift_cluster.RedshiftResumeClusterOperatorAsync`.

.. exampleinclude:: /../astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_redshift_resume_cluster_async]
    :end-before: [END howto_operator_redshift_resume_cluster_async]
