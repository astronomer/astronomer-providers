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
     - ``from astronomer.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_sql.py>`_
   * - ``RedshiftPauseClusterOperatorAsync``
     - ``from astronomer.providers.amazon.aws.operators.redshift_cluster import RedshiftPauseClusterOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py>`_
   * - ``RedshiftResumeClusterOperatorAsync``
     - ``from astronomer.providers.amazon.aws.operators.redshift_cluster import RedshiftResumeClusterOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py>`_
   * - ``RedshiftClusterSensorAsync``
     - ``from astronomer.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_redshift_cluster_management.py>`_
   * - ``S3KeySensorAsync``
     - ``from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/amazon/aws/example_dags/example_s3.py>`_
   * - ``KubernetesPodOperatorAsync``
     - ``from astronomer_operators.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/cncf/kubernetes/example_dags/example_kubernetes_pod_operator.py>`_
   * - ``ExternalTaskSensorAsync``
     - ``from astronomer_operators.core.sensors.external_task import ExternalTaskSensorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/core/example_dags/example_external_task.py>`_
   * - ``FileSensorAsync``
     - ``from astronomer_operators.core.sensors.filesystem import FileSensorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/core/example_dags/example_file_sensor.py>`_
   * - ``DatabricksRunNowOperatorAsync``
     - ``from astronomer.providers.databricks.operators.databricks import DatabricksRunNowOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/databricks/example_dags/example_databricks.py>`_
   * - ``DatabricksSubmitRunOperatorAsync``
     - ``from astronomer.providers.databricks.operators.databricks import DatabricksSubmitRunOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/databricks/example_dags/example_databricks.py>`_
   * - ``BigQueryCheckOperatorAsync``
     - ``from astronomer.providers.google.cloud.operators.bigquery import BigQueryCheckOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`_
   * - ``BigQueryGetDataOperatorAsync``
     - ``from astronomer.providers.google.cloud.operators.bigquery import BigQueryGetDataOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`_
   * - ``BigQueryInsertJobOperatorAsync``
     - ``from astronomer.providers.google.cloud.operators.bigquery import  BigQueryInsertJobOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`_
   * - ``BigQueryIntervalCheckOperatorAsync``
     - ``from astronomer.providers.google.cloud.operators.bigquery import BigQueryIntervalCheckOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`_
   * - ``BigQueryValueCheckOperatorAsync``
     - ``from astronomer.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_bigquery_queries.py>`_
   * - ``GCSObjectExistenceSensorAsync``
     - ``from astronomer.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/google/cloud/example_dags/example_gcs.py>`_
   * - ``HttpSensorAsync``
     - ``from astronomer.providers.http.sensors.http import HttpSensorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/http/example_dags/example_http.py>`_
   * - ``SnowflakeOperatorAsync``
     - ``from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync``
     - `Example DAG <https://github.com/astronomer/astronomer-providers/blob/1.0.0/astronomer/providers/snowflake/example_dags/example_snowflake.py>`_
