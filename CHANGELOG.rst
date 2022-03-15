Changelog
=========

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
