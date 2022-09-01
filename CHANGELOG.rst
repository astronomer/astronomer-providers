Changelog
=========

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
