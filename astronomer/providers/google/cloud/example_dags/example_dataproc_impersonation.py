"""Example Airflow DAG which uses impersonation parameters for authenticating with Dataproc operators."""

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)

from astronomer.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperatorAsync,
    DataprocDeleteClusterOperatorAsync,
    DataprocSubmitJobOperatorAsync,
    DataprocUpdateClusterOperatorAsync,
)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "astronomer-airflow-providers")
CLUSTER_NAME = os.getenv("GCP_DATAPROC_CLUSTER_NAME", "example-cluster-astronomer-providers")
REGION = os.getenv("GCP_LOCATION", "us-central1")
GCP_IMPERSONATION_CONN_ID = os.getenv("GCP_IMPERSONATION_CONN_ID", "google_impersonation")
ZONE = os.getenv("GCP_REGION", "us-central1-a")
BUCKET = os.getenv("GCP_DATAPROC_BUCKET", "dataproc-system-tests-astronomer-providers")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = f"gs://{BUCKET}/{OUTPUT_FOLDER}/"
IMPERSONATION_CHAIN = os.getenv("IMPERSONATION_CHAIN", "")

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]


CLUSTER_UPDATE = {"config": {"worker_config": {"num_instances": 2}}}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

# Jobs definitions
# [START how_to_cloud_dataproc_pig_config]
PIG_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pig_job": {"query_list": {"queries": ["define sin HiveUDF('sin');"]}},
}
# [END how_to_cloud_dataproc_pig_config]

# [START how_to_cloud_dataproc_sparksql_config]
SPARK_SQL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_sql_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}
# [END how_to_cloud_dataproc_sparksql_config]

# [START how_to_cloud_dataproc_spark_config]
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}
# [END how_to_cloud_dataproc_spark_config]

# [START how_to_cloud_dataproc_hive_config]
HIVE_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hive_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}
# [END how_to_cloud_dataproc_hive_config]

# [START how_to_cloud_dataproc_hadoop_config]
HADOOP_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar",
        "args": ["wordcount", "gs://pub/shakespeare/rose.txt", OUTPUT_PATH],
    },
}
# [END how_to_cloud_dataproc_hadoop_config]
WORKFLOW_NAME = "airflow-dataproc-test"
WORKFLOW_TEMPLATE = {
    "id": WORKFLOW_NAME,
    "placement": {
        "managed_cluster": {
            "cluster_name": CLUSTER_NAME,
            "config": CLUSTER_CONFIG,
        }
    },
    "jobs": [{"step_id": "pig_job_1", "pig_job": PIG_JOB["pig_job"]}],
}

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


with models.DAG(
    dag_id="example_gcp_dataproc_impersonation",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "dataproc"],
) as dag:
    # [START howto_operator_dataproc_create_cluster_async]
    create_cluster = DataprocCreateClusterOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_operator_dataproc_create_cluster_async]

    # [START howto_operator_dataproc_update_cluster_async]
    update_cluster = DataprocUpdateClusterOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="update_cluster",
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER_UPDATE,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout=TIMEOUT,
        project_id=PROJECT_ID,
        region=REGION,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_operator_dataproc_update_cluster_async]

    # [START howto_create_bucket_task]
    create_bucket = GCSCreateBucketOperator(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="create_bucket",
        bucket_name=BUCKET,
        project_id=PROJECT_ID,
        resource={
            "iamConfiguration": {
                "uniformBucketLevelAccess": {
                    "enabled": False,
                },
            },
        },
    )
    # [END howto_create_bucket_task]

    # [START howto_operator_dataproc_submit_pig_job_async]
    pig_task = DataprocSubmitJobOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="pig_task",
        job=PIG_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_dataproc_submit_pig_job_async]

    # [START howto_DataprocSubmitJobOperatorAsync]
    spark_sql_task = DataprocSubmitJobOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="spark_sql_task",
        job=SPARK_SQL_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START howto_DataprocSubmitJobOperatorAsync]
    spark_task = DataprocSubmitJobOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="spark_task",
        job=SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START howto_DataprocSubmitJobOperatorAsync]
    hive_task = DataprocSubmitJobOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="hive_task",
        job=HIVE_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START howto_DataprocSubmitJobOperatorAsync]
    hadoop_task = DataprocSubmitJobOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="hadoop_task",
        job=HADOOP_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_DataprocSubmitJobOperatorAsync]

    # [START howto_operator_dataproc_delete_cluster_async]
    delete_cluster = DataprocDeleteClusterOperatorAsync(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_operator_dataproc_delete_cluster_async]

    # [START howto_delete_buckettask]
    delete_bucket = GCSDeleteBucketOperator(
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        task_id="delete_bucket",
        bucket_name=BUCKET,
        trigger_rule="all_done",
    )
    # [END howto_delete_buckettask]

    end = EmptyOperator(task_id="end")

    create_cluster >> update_cluster >> hive_task >> spark_task >> spark_sql_task >> delete_cluster
    (
        create_cluster
        >> update_cluster
        >> pig_task
        >> create_bucket
        >> hadoop_task
        >> delete_bucket
        >> delete_cluster
    )

    [spark_sql_task, hadoop_task, delete_cluster, delete_bucket] >> end
