Changelog
=========

1.19.2 (2024-07-29)
-------------------

Bug Fixes
"""""""""

* Fix ``SnowflakeSensorTrigger`` storing callable as string (#1530)


1.19.1 (2024-05-10)
-------------------

Bug Fixes
"""""""""
* Pass ``parameters`` to parent class in ``SnowflakeSensorAsync`` (#1519)


1.19.0 (2024-02-21)
-------------------

* Add ExternalDeploymentSensor (#1472)
* Deprecate the following operators and sensors and proxy them to their upstream Apache Airflow providers' deferrable counterparts
    * EmrContainerSensorAsync
    * EmrStepSensorAsync
    * EmrJobFlowSensorAsync
    * RedshiftClusterSensorAsync
    * BatchSensorAsync
    * S3KeySensorAsync
    * S3KeysUnchangedSensorAsync
    * EmrContainerOperatorAsync
    * RedshiftDeleteClusterOperatorAsync
    * RedshiftResumeClusterOperatorAsync
    * RedshiftPauseClusterOperatorAsync
    * BatchOperatorAsync
    * SageMakerProcessingOperatorAsync
    * SageMakerTransformOperatorAsync
    * SageMakerTrainingOperatorAsync
    * RedshiftDataOperatorAsync
    * GCSObjectExistenceSensorAsync
    * GCSObjectsWithPrefixExistenceSensorAsync
    * GCSUploadSessionCompleteSensorAsync
    * GCSObjectUpdateSensorAsync
    * GKEStartPodOperatorAsync
    * BigQueryInsertJobOperatorAsync
    * BigQueryCheckOperatorAsync
    * BigQueryGetDataOperatorAsync
    * BigQueryIntervalCheckOperatorAsync
    * BigQueryValueCheckOperatorAsync
    * DataprocCreateClusterOperatorAsync
    * DataprocDeleteClusterOperatorAsync
    * DataprocSubmitJobOperatorAsync
    * DataprocUpdateClusterOperatorAsync
    * BigQueryTableExistenceSensorAsync
    * AzureDataFactoryPipelineRunStatusSensorAsync
    * WasbBlobSensorAsync
    * WasbPrefixSensorAsync
    * AzureDataFactoryRunPipelineOperatorAsync
    * FileSensorAsync
    * ExternalTaskSensorAsync
    * SnowflakeSqlApiOperatorAsync
    * DatabricksSubmitRunOperatorAsync
    * DatabricksRunNowOperatorAsync
    * KubernetesPodOperatorAsync
    * SFTPSensorAsync
    * HttpSensorAsync
    * DbtCloudJobRunSensorAsync
    * DbtCloudRunJobOperatorAsync
    * LivyOperatorAsync
* Add deprecation notice docs for astronomer-providers (#1468)
* Increase min airflow version to 2.6 (#1438)
* Bump mininium python to 3.8 (#1442)


1.18.4 (2023-12-07)
-------------------

Bug Fixes
"""""""""
* DatabricksHookAsync: include 403 as retryable error (#1376)

1.18.3 (2023-11-29)
-------------------

Misc
"""""
* Revert "Pin max versions for CNCF Kubernetes and Google Airflow providers (#1359)" (#1368)

1.18.2 (2023-11-11)
-------------------

Bug Fixes
"""""""""
* Pin max versions for CNCF Kubernetes and Google Airflow providers (#1359)
* Pin ``connexion==2.14.2`` due to issue with latest release version3.0.0 (#1354)

1.18.1 (2023-11-01)
-------------------

Bug Fixes
"""""""""
* Handle ``ClientConnectorError`` to allow for retries (#1338)
* Make ``resource_group_name`` and ``factory_name`` required in Azure DataFactory hook and trigger (#1341)


1.18.0 (2023-09-25)
-------------------

Enhancements
""""""""""""
* Add ``keep_response`` parameter to ``HttpHookAsync``  (#1330)
* Add AioRefreshableCredentials to AwsBaseHookAsync (#1304)

Bug Fixes
"""""""""
* Fix ExternalDeploymentTaskSensorAsync by making simple http requests instead of aiohttp (#1306)
* Fix impersonation failure on GKE start pod operator async (#1274)

Misc
"""""
* Bring back hive provider for Python3.11 (#1317)


1.17.3 (2023-08-07)
-------------------

Bug Fixes
"""""""""
* Raise ``AirflowSkipException`` for sensors when ``soft_fail`` is set to True(#1276)
* ``S3KeyTrigger``: Call S3 sensor check_fn only with file size attribute (#1278)
* ``HttpSensorAsync``: Fix incorrect warning when poke_interval is passed (#1281)

Note
"""""""""
* ``hive`` provider is not supported on Python 3.11 (#1237)

1.17.2 (2023-07-26)
-------------------

Bug Fixes
"""""""""
* ``RedshiftDataHook``: remove snowflake dependencies and use DbApiHook.split_sql_string for parsing sql
* ``DatabricksRunNowOperatorAsync``: get job_id through Databricks API if job_name is passed

1.17.1 (2023-06-22)
-------------------

Bug Fixes
"""""""""
* Revert "feat(kubernetes): check state before deferring KubernetesPodOperatorAsync (#1104)" (`#1209 <https://github.com/astronomer/astronomer-providers/pull/1209>`_)

1.17.0 (2023-06-21)
-------------------

Enhancements
""""""""""""

* Enhance ``S3KeySizeSensorAsync``

  * Add ``use_regex`` param  (`#1172 <https://github.com/astronomer/astronomer-providers/pull/1172>`_)

  * Handle soft fail (`#1161 <https://github.com/astronomer/astronomer-providers/pull/1161>`_)

Bug Fixes
"""""""""
- Fixing the issue that ``check_fn`` was ignored in ``S3KeySensorAsync`` (`#1171 <https://github.com/astronomer/astronomer-providers/pull/1171>`_)


1.16.0 (2023-05-19)
-------------------

Enhancements
""""""""""""

* Enhance ``SFTPSensorAsync`` by adding below features (`#1072 <https://github.com/astronomer/astronomer-providers/pull/1072>`_):

  * Remove the need for prefixing ``ssh-`` to host keys that don't have such prefix e.g. ecdsa type keys

  * Support validating host keys using a known_hosts file

  * Accept string values for ``newer_than`` field. e.g. passed via Jinja template

  * Use ``readdir`` for listing files instead of ``stat`` on each file to avoid throttling caused by multiple roundtrips to the server for each file

Bug Fixes
"""""""""

- Mark ``DbtCloudRunJobOperatorAsync`` failed if cancelled by raising ``AirflowFailException`` (`#1082 <https://github.com/astronomer/astronomer-providers/pull/1082>`_)


1.15.5 (2023-04-24)
-------------------

Bug Fixes
"""""""""

- Support host key verification for ``SFTPSensorAsync`` (`#963 <https://github.com/astronomer/astronomer-providers/pull/963>`_)
- Make BigQuery & Google Cloud Storage async operators & sensors compatible with ``apache-airflow-providers-google>=10.0.0``
  (`#981 <https://github.com/astronomer/astronomer-providers/pull/981>`_)
- Make ``SageMakerProcessingOperatorAsync`` compatible with ``apache-airflow-providers-amazon>=8.0.0``
  (`#979 <https://github.com/astronomer/astronomer-providers/pull/979>`_)
- Make ``BatchOperatorAsync`` compatible with ``apache-airflow-providers-amazon>=8.0.0``


1.15.4 (2023-04-19)
-------------------

Bug Fixes
"""""""""

- Fix backward compatibility issue with BigQuery Async Operators
  (`#967 <https://github.com/astronomer/astronomer-providers/pull/967>`_)


1.15.3 (2023-04-17)
-------------------

Bug Fixes
"""""""""

- Allow and prefer non-prefixed extra fields for ``KubernetesHookAsync``
  (`#944 <https://github.com/astronomer/astronomer-providers/pull/944>`_)
- Make ``GKEStartPodOperatorAsync`` compatible with ``apache-airflow-providers-google>=9.0.0``.
  (`#954 <https://github.com/astronomer/astronomer-providers/pull/954>`_)
- BigQuery Async Operators accepts poll_interval as argument to override trigger's default poll_interval.
  (`#953 <https://github.com/astronomer/astronomer-providers/pull/953>`_)
- Fix S3 and GCS custom XCOM backend json loading issue
  (`#961 <https://github.com/astronomer/astronomer-providers/pull/961>`_)
- Pinning snowflake-sqlalchemy to greater than or equal to 1.4.4
  (`#962 <https://github.com/astronomer/astronomer-providers/pull/962>`_)


1.15.2 (2023-03-27)
-------------------

Bug Fixes
"""""""""

- Fix ``S3KeyTrigger`` to return only one trigger event when ``check_fn`` is set to none.
  (`#925 <https://github.com/astronomer/astronomer-providers/pull/925>`_)
- Handle unclosed connection errors in ``HttpTrigger`` .
  (`#927 <https://github.com/astronomer/astronomer-providers/pull/927>`_)


1.15.1 (2023-03-09)
-------------------

Bug Fixes
"""""""""

- Allow and prefer non-prefixed extra fields for ``AzureDataFactoryHookAsync``.
  (`#899 <https://github.com/astronomer/astronomer-providers/pull/899>`_)
- Fix ``HttpSensorAsync`` to use the correct connection id instead of a default connection id
  (`#896 <https://github.com/astronomer/astronomer-providers/pull/896>`_)
- Fix ``SFTPSensorAsync`` to succeed when there is at least one file newer than the provided date
  and set ``SFTPHookAsync`` default_known_hosts parameter to None.
  (`#905 <https://github.com/astronomer/astronomer-providers/pull/905>`_)
- Make ``SageMakerTransformOperatorAsync`` and ``SageMakerTrainingOperatorAsync`` compatible with ``apache-airflow-providers-amazon>=7.3.0``.
  (`#912 <https://github.com/astronomer/astronomer-providers/pull/912>`_)
- Fix ``BigQueryInsertJobOperatorAsync`` to send location parameter to hook.
  (`#866 <https://github.com/astronomer/astronomer-providers/pull/866>`_)
- Mitigate race condition on deferrable sensors ``ExternalTaskSensorAsync`` and ``SnowflakeSensorAsync`` immediately getting successful,
  by making the changes to these sensors to check on the worker first, then defer if the condition is not met.
  (`#908 <https://github.com/astronomer/astronomer-providers/pull/908>`_)


1.15.0 (2023-02-15)
-------------------

New Operators
"""""""""""""

This release adds a new async sensor ``SnowflakeSensorAsync``.

.. list-table::
   :header-rows: 1

   * - Sensor Class
     - Import Path
     - Example DAG

   * - ``SnowflakeSensorAsync``
     - .. code-block:: python

        from astronomer.providers.snowflake.sensors.snowflake import SnowflakeSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/snowflake/example_dags/example_snowflake_sensor.py>`__

Enhancements
""""""""""""

- Support tags param in ``EmrContainerOperatorAsync``.
  (`#836 <https://github.com/astronomer/astronomer-providers/pull/836>`_)
- Enhance ``DbtCloudHookAsync`` to accept tenant domain name while making connection.
  (`#855 <https://github.com/astronomer/astronomer-providers/pull/855>`_)

Bug Fixes
"""""""""

- Ensure ``DataprocLink`` is visible for tracking the dataproc jobs status in ``DataprocSubmitJobOperatorAsync``.
  (`#835 <https://github.com/astronomer/astronomer-providers/pull/835>`_)
- Handle ``SnowflakeHookAsync`` when a empty sql list is passed`.
  (`#838 <https://github.com/astronomer/astronomer-providers/pull/838>`_)
- Use ``timeout`` instead of ``execution_timeout`` with ``ExternalTaskSensor`` when sensing task_id`.
  (`#858 <https://github.com/astronomer/astronomer-providers/pull/858>`_)


1.14.0 (2023-01-09)
-------------------

Feature
"""""""

- Add custom XCom backend for S3.
  (`#820 <https://github.com/astronomer/astronomer-providers/pull/820>`_)

Bug Fixes
"""""""""

- Add poke_interval to ``ExternalTaskSensorAsync``.
  (`#823 <https://github.com/astronomer/astronomer-providers/pull/823>`_)
- Support getting logs of pod, pod cleanup and reflect status of the pod in ``GKEStartPodOperatorAsync``
  (`#824 <https://github.com/astronomer/astronomer-providers/pull/824>`_)


1.13.0 (2022-12-16)
-------------------

Feature
"""""""

- Enhance ``AwsBaseHookAsync`` to support assume role ``role_arn`` passed via ``extra_config`` in the airflow connection.
  (`#804 <https://github.com/astronomer/astronomer-providers/pull/804>`_)
- Support private key authentication for ``SFTPHookAsync`` via SFTP connection.
  (`#749 <https://github.com/astronomer/astronomer-providers/pull/749>`_)

Bug Fixes
"""""""""

- Fix ``KubernetesPodOperatorAsync`` to consider kubernetes connection id in the trigger.
  (`#815 <https://github.com/astronomer/astronomer-providers/pull/815>`_)


1.12.0 (2022-12-02)
-------------------

Feature
"""""""

- Add custom XCom backend for GCS.
  (`#681 <https://github.com/astronomer/astronomer-providers/pull/681>`_)

Bug Fixes
"""""""""

- Change return value of ``SnowflakeOperatorAsync`` to be same as that of ``SnowflakeOperator``.
  (`#781 <https://github.com/astronomer/astronomer-providers/pull/781>`_)
- Add poke_interval to ``S3KeySensorAsync``.
  (`#782 <https://github.com/astronomer/astronomer-providers/pull/782>`_)
- Use ``AwsConnectionWrapper`` to get connection details to create AWS async client.
  (`#758 <https://github.com/astronomer/astronomer-providers/pull/758>`_)
- Fix ADF Sensor broken docs.
  (`#779 <https://github.com/astronomer/astronomer-providers/pull/779>`_)
- Log warning message when ``response_check`` attribute is passed in ``HttpSensorAsync``.
  (`#780 <https://github.com/astronomer/astronomer-providers/pull/780>`_)


1.11.2 (2022-11-19)
-------------------

Bug Fixes
"""""""""

- Handle ``SFTPSensorAsync`` failure when file pattern is not passed.
  (`#744 <https://github.com/astronomer/astronomer-providers/pull/744>`_)
- Fix ``RedshiftDataHook`` to accept AWS access and secret keys from the connection object.
  (`#746 <https://github.com/astronomer/astronomer-providers/pull/746>`_)
- Make ``BigQueryHookAsync`` compatible with ``apache-airflow-providers-google>=8.5.0``.
  (`#751 <https://github.com/astronomer/astronomer-providers/pull/751>`_)
- Make ``RedshiftSQLOperatorAsync`` compatible with ``apache-airflow-providers-amazon>=6.1.0``.
  (`#762 <https://github.com/astronomer/astronomer-providers/pull/762>`_)
- Make ``SnowflakeOperatorAsync`` and ``SnowflakeSqlApiOperatorAsync`` compatible with ``apache-airflow-providers-snowflake>=4.0.0``.
  (`#767 <https://github.com/astronomer/astronomer-providers/pull/767>`_)


1.11.1 (2022-10-28)
-------------------

Bug Fixes
"""""""""

- Fix ``AwsBaseHookAsync`` to accept the AWS session token passed via ``extra_config`` in the airflow connection
  (`#730 <https://github.com/astronomer/astronomer-providers/pull/730>`_)
- Change return value of ``SageMakerTrainingOperatorAsync``  and ``SageMakerTransformOperatorAsync``
  to be same as that of corresponding Sync operators
  (`#737 <https://github.com/astronomer/astronomer-providers/pull/737>`_)


1.11.0 (2022-10-21)
-------------------

New Operators
"""""""""""""

This release adds the following 3 new async operators:

.. list-table::
   :header-rows: 1

   * - Operator Class
     - Import Path
     - Example DAG

   * - ``SageMakerProcessingOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.sagemaker import SageMakerProcessingOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_sagemaker.py>`__

   * - ``SageMakerTrainingOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.sagemaker import SageMakerTrainingOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_sagemaker.py>`__

   * - ``SageMakerTransformOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_sagemaker.py>`__



1.10.0 (2022-09-30)
-------------------

New Operators
"""""""""""""

This release adds the following 2 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``ExternalDeploymentTaskSensorAsync``
     - .. code-block:: python

        from astronomer.providers.core.sensors.external_task import ExternalDeploymentTaskSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/core/example_dags/example_external_deployment_task_sensor.py>`__

   * - ``SFTPSensorAsync``
     - .. code-block:: python

        from astronomer.providers.sftp.sensors.sftp import SFTPSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/sftp/example_dags/example_sftp.py>`__

Bug Fixes
"""""""""

- Make Dataproc operator compatible with ``apache-airflow-providers-google>=8.4.0``
  (`#680 <https://github.com/astronomer/astronomer-providers/pull/680>`_)
- Make EMR EKS operator compatible with ``apache-airflow-providers-amazon>=6.0.0``
  (`#682 <https://github.com/astronomer/astronomer-providers/pull/682>`_)

Deprecation
"""""""""""

- Deprecate ``poll_interval`` and use ``poke_interval`` for all async sensors
  (`#640 <https://github.com/astronomer/astronomer-providers/pull/640>`_)


1.9.0 (2022-09-13)
------------------

New Operators
"""""""""""""

This release adds the following 2 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``DbtCloudJobRunSensorAsync``
     - .. code-block:: python

        from astronomer.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/dbt/cloud/example_dags/example_dbt_cloud.py>`__

   * - ``DbtCloudRunJobOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/dbt/cloud/example_dags/example_dbt_cloud.py>`__


Bug Fixes
"""""""""

- Include ``astronomer-providers`` in the Providers view within the Airflow UI
  (`#626 <https://github.com/astronomer/astronomer-providers/pull/626>`_)

Enhancements
""""""""""""

- Implement OpenLineage custom extractor for Redshift Async Operators
  (`#561 <https://github.com/astronomer/astronomer-providers/pull/561>`_)


1.8.1 (2022-09-01)
------------------

Bug Fixes
"""""""""

- Fix timeout errors on ``AzureDataFactoryRunPipelineOperatorAsync``
  (`#602 <https://github.com/astronomer/astronomer-providers/pull/602>`_)
- Remove ``werkzeug`` dep & limit ``protobuf`` to ``3.20.0`` (`#615 <https://github.com/astronomer/astronomer-providers/pull/615>`_)
- Raise exception in case of user error in async Databricks Operator
  (`#612 <https://github.com/astronomer/astronomer-providers/pull/612>`_)


1.8.0 (2022-08-16)
------------------

Bug Fixes
"""""""""

- Add poll interval to ``HttpSensorAsync``
  (`#554 <https://github.com/astronomer/astronomer-providers/pull/554>`_)
- Replace execution_timeout with timeout in all the async sensors
  (`#555 <https://github.com/astronomer/astronomer-providers/pull/555>`_)
- Get default 'resource_group_name' and 'factory_name' for
  AzureDataFactoryPipelineRunStatusSensorAsync
  (`#589 <https://github.com/astronomer/astronomer-providers/pull/589>`_)

Enhancements
""""""""""""

- Add elaborate documentation and use cases for ``SnowflakeOperatorAsync``
  (`#556 <https://github.com/astronomer/astronomer-providers/pull/556>`_)
- Improve telemetry for Async Databricks Operators
  (`#582 <https://github.com/astronomer/astronomer-providers/pull/582>`_)
- Enhance ``S3KeySensorAsync`` to accept multiple keys and
  deprecate ``S3PrefixSensorAsync`` and ``S3KeySizeSensorAsync``
  (`#577 <https://github.com/astronomer/astronomer-providers/pull/577>`_)


1.7.1 (2022-07-25)
------------------

Bug Fixes
"""""""""

- Bump up  ``MarkupSafe`` version as per Airflow 2.3.3 constraints
  (`#542 <https://github.com/astronomer/astronomer-providers/pull/542>`_)
- Downgrade ``Werkzeug`` version below 2.2.0 as it causes
  ``ImportError: cannot import name 'parse_rule' from 'werkzeug.routing'``
  (`#551 <https://github.com/astronomer/astronomer-providers/pull/551>`_)


1.7.0 (2022-07-19)
------------------

New Operators
"""""""""""""

This release adds the following 4 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``BatchSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.batch import BatchSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_batch.py>`__

   * - ``SnowflakeSqlApiOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/snowflake/example_dags/example_snowflake_sql_api.py>`__

   * - ``WasbBlobSensorAsync``
     - .. code-block:: python

        from astronomer.providers.microsoft.azure.sensors.wasb import WasbBlobSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/microsoft/azure/example_dags/example_wasb_sensors.py>`__

   * - ``WasbPrefixSensorAsync``
     - .. code-block:: python

        from astronomer.providers.microsoft.azure.sensors.wasb import WasbPrefixSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/microsoft/azure/example_dags/example_wasb_sensors.py>`__


Enhancements
""""""""""""

- Add copy button to code blocks in docs
  (`#505 <https://github.com/astronomer/astronomer-providers/pull/505>`_)
- Add custom Sphinx extension to list available operators & sensors
  (`#504 <https://github.com/astronomer/astronomer-providers/pull/504>`_)
- Add pre-commit hook to check for dead links in markdown files
  (`#524 <https://github.com/astronomer/astronomer-providers/pull/524>`_)



1.6.0 (2022-06-28)
------------------

New Operators
"""""""""""""

This release adds the following 5 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``DataprocCreateClusterOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_dataproc.py>`__

   * - ``DataprocDeleteClusterOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_dataproc.py>`__

   * - ``DataprocUpdateClusterOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.dataproc import DataprocUpdateClusterOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_dataproc.py>`__

   * - ``RedshiftDataOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.redshift_data import RedshiftDataOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_redshift_data.py>`__

   * - ``RedshiftDeleteClusterOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.redshift_cluster import RedshiftDeleteClusterOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py>`__

Enhancements
""""""""""""

- Implement OpenLineage custom extractor for BigQuery Async Operators
  (`#429 <https://github.com/astronomer/astronomer-providers/pull/429>`_)
- Add session specific query tag and OpenLineage Extractor for Snowflake Async operator
  (`#437 <https://github.com/astronomer/astronomer-providers/pull/437>`_)
- Handle ``DataprocCreateClusterOperatorAsync`` errors gracefully and add additional
  functionality with ``use_if_exists`` and ``delete_on_error`` parameters
  (`#448 <https://github.com/astronomer/astronomer-providers/pull/448>`_)

Bug Fixes
"""""""""

- Fix ``BigQueryInsertJobOperatorAsync`` failure after Google provider upgrade to 8.1.0
  (`#471 <https://github.com/astronomer/astronomer-providers/pull/471>`_)

1.5.0 (2022-06-15)
------------------

This release adds the following 2 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``BatchOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.batch import BatchOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_batch.py>`__

   * - ``GKEStartPodOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_kubernetes_engine.py>`__

Improvements
""""""""""""

* Enhance **KubernetesPodOperatorAsync** to periodically resume the sync portion of the task to fetch and
  emit the latest logs before deferring again.
  (`#139 <https://github.com/astronomer/astronomer-providers/pull/139>`_)
* Fix a bug on the  **KubernetesPodOperatorAsync**  to not fail with ``ERROR - Unclosed client session``
  (`#394 <https://github.com/astronomer/astronomer-providers/pull/394>`_)


1.4.0 (2022-05-25)
------------------

Enhancements
""""""""""""

- Enable Kerberos Authentication in ``HivePartitionSensorAsync`` and
  ``NamedHivePartitionSensorAsync``
  (`#357 <https://github.com/astronomer/astronomer-providers/pull/357>`_)


Bug Fixes
"""""""""

- Fix example Redshift DAGs to catch appropriate exception during cluster deletion
  (`#348 <https://github.com/astronomer/astronomer-providers/pull/348>`_)
- Move ``xcom_push`` call to ``execute`` method for all async operators
  (`#371 <https://github.com/astronomer/astronomer-providers/pull/371>`_)




1.3.1 (2022-05-22)
------------------

Bug Fixes
"""""""""

- Correct module name for ``DagStateTrigger`` which prevented use of
  ``ExternalTaskSensorAsync`` when ``external_task_id`` was not passed
  (`#361 <https://github.com/astronomer/astronomer-providers/pull/361>`_)
- Add ``template_fields`` to ``S3KeySensorAsync`` (`#373 <https://github.com/astronomer/astronomer-providers/pull/373>`_)

Docs
""""

- Add missing Extras in ``README.rst`` and automate it (`#329 <https://github.com/astronomer/astronomer-providers/pull/329>`_)

Misc
""""

- Improvements in Example DAGs (Hive, Livy)
  (`#342 <https://github.com/astronomer/astronomer-providers/pull/342>`_,
  `#348 <https://github.com/astronomer/astronomer-providers/pull/348>`_,
  `#349 <https://github.com/astronomer/astronomer-providers/pull/349>`_)

1.3.0 (2022-05-09)
------------------

New Operators
"""""""""""""

This release adds the following 5 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``AzureDataFactoryRunPipelineOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/microsoft/azure/example_dags/example_adf_run_pipeline.py>`__

   * - ``AzureDataFactoryPipelineRunStatusSensorAsync``
     - .. code-block:: python

        from astronomer.providers.microsoft.azure.operators.data_factory import AzureDataFactoryPipelineRunStatusSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/microsoft/azure/example_dags/example_adf_run_pipeline.py>`__

   * - ``EmrContainerOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.emr import EmrContainerOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_emr_eks_containers_job.py>`__

   * - ``HivePartitionSensorAsync``
     - .. code-block:: python

        from astronomer.providers.apache.hive.sensors.hive_partition import HivePartitionSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/apache/hive/example_dags/example_hive.py>`__

   * - ``NamedHivePartitionSensorAsync``
     - .. code-block:: python

        from astronomer.providers.apache.hive.sensors.named_hive_partition import NamedHivePartitionSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/apache/hive/example_dags/example_hive.py>`__


Improvements
""""""""""""

* Improved example DAGs so that minimal resources are created during integration tests
* Fixes a bug on the  **DatabricksRunNowOperatorAsync**  to check event status correctly
  (`#251 <https://github.com/astronomer/astronomer-providers/pull/251>`_)

1.2.0 (2022-04-12)
------------------

New Operators
"""""""""""""

This release adds the following 5 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``DataprocSubmitJobOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_dataproc.py>`__

   * - ``EmrContainerSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.emr import EmrContainerSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_emr.py>`__

   * - ``EmrStepSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.emr import EmrStepSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_emr_sensor.py>`__

   * - ``EmrJobFlowSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.emr import EmrJobFlowSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_emr_sensor.py>`__

   * - ``LivyOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.apache.livy.operators.livy import LivyOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/apache/livy/example_dags/example_livy.py>`__


Improvements
""""""""""""

* Improved example DAGs so that resource creation and clean up is handled during system tests rather
  than doing it manually
* Enhanced the  **Async Databricks Operator**  to persist ``run_id`` and ``run_page_url`` in ``XCom``
  (`#175 <https://github.com/astronomer/astronomer-providers/pull/175>`_)


1.1.0 (2022-03-23)
--------------------

New Operators
"""""""""""""

This release adds the following 7 new async sensors/operators:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG

   * - ``S3KeySizeSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.s3 import S3KeySizeSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_s3.py>`__

   * - ``S3KeysUnchangedSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.s3 import S3KeysUnchangedSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_s3.py>`__

   * - ``S3PrefixSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.s3 import S3PrefixSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/amazon/aws/example_dags/example_s3.py>`__

   * - ``GCSObjectsWithPrefixExistenceSensorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_gcs.py>`__

   * - ``GCSObjectUpdateSensorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_gcs.py>`__

   * - ``GCSUploadSessionCompleteSensorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.sensors.gcs import GCSUploadSessionCompleteSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_gcs.py>`__

   * - ``BigQueryTableExistenceSensorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/main/astronomer/providers/google/cloud/example_dags/example_bigquery_sensors.py>`__



Improvements
""""""""""""

The dependencies for installing this repo are now split into multiple extras as follows (`#113 <https://github.com/astronomer/astronomer-providers/pull/113>`__)

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Installation Command
     - Dependencies
   * - ``all``
     - ``pip install 'astronomer-providers[all]'``
     - All providers
   * - ``amazon``
     - ``pip install 'astronomer-providers[amazon]'``
     - Amazon
   * - ``cncf.kubernetes``
     - ``pip install 'astronomer-providers[cncf.kubernetes]'``
     - Kubernetes
   * - ``databricks``
     - ``pip install 'astronomer-providers[databricks]'``
     - Databricks
   * - ``google``
     - ``pip install 'astronomer-providers[google]'``
     - Google Cloud
   * - ``http``
     - ``pip install 'astronomer-providers[http]'``
     - HTTP
   * - ``snowflake``
     - ``pip install 'astronomer-providers[snowflake]'``
     - Snowflake

This will allow users to just install dependencies of a single provider. For example, if a user
wants to just use ``KubernetesPodOperatorAsync``, they should not need to install GCP, AWS or
Snowflake dependencies by running ``pip install 'astronomer-providers[cncf.kubernetes]'``.

Bug Fixes
"""""""""

* Fixes a bug on the **Async Databricks Triggerer** failing due to malformed authentication
  header along with improved exception handling to send the Triggerer errors back to the worker to understand
  why a particular job execution has failed. (`#147 <https://github.com/astronomer/astronomer-providers/pull/147>`_)

1.0.0 (2022-03-01)
------------------

* Initial release, with the following **18** Async Operators/Sensors:

.. list-table::
   :header-rows: 1

   * - Operator/Sensor Class
     - Import Path
     - Example DAG
   * - ``RedshiftSQLOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_sql.py>`__
   * - ``RedshiftPauseClusterOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.redshift_cluster import RedshiftPauseClusterOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py>`__
   * - ``RedshiftResumeClusterOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.operators.redshift_cluster import RedshiftResumeClusterOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py>`__
   * - ``RedshiftClusterSensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py>`__
   * - ``S3KeySensorAsync``
     - .. code-block:: python

        from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_s3.py>`__
   * - ``KubernetesPodOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/cncf/kubernetes/example_dags/example_kubernetes_pod_operator.py>`__
   * - ``ExternalTaskSensorAsync``
     - .. code-block:: python

        from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/core/example_dags/example_external_task.py>`__
   * - ``FileSensorAsync``
     - .. code-block:: python

        from astronomer.providers.core.sensors.filesystem import FileSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/core/example_dags/example_file_sensor.py>`__
   * - ``DatabricksRunNowOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.databricks.operators.databricks import DatabricksRunNowOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/databricks/example_dags/example_databricks.py>`__
   * - ``DatabricksSubmitRunOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.databricks.operators.databricks import DatabricksSubmitRunOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/databricks/example_dags/example_databricks.py>`__
   * - ``BigQueryCheckOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.bigquery import BigQueryCheckOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`__
   * - ``BigQueryGetDataOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.bigquery import BigQueryGetDataOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`__
   * - ``BigQueryInsertJobOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.bigquery import  BigQueryInsertJobOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`__
   * - ``BigQueryIntervalCheckOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.bigquery import BigQueryIntervalCheckOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`__
   * - ``BigQueryValueCheckOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`__
   * - ``GCSObjectExistenceSensorAsync``
     - .. code-block:: python

        from astronomer.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_gcs.py>`__
   * - ``HttpSensorAsync``
     - .. code-block:: python

        from astronomer.providers.http.sensors.http import HttpSensorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/http/example_dags/example_http.py>`__
   * - ``SnowflakeOperatorAsync``
     - .. code-block:: python

        from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/snowflake/example_dags/example_snowflake.py>`__
