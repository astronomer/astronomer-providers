Changelog
=========

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
