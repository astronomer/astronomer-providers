"""Example Airflow DAG that shows how to use DataprocSubmitJobOperatorAsync."""

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocCreateWorkflowTemplateOperator,
    DataprocDeleteClusterOperator,
    DataprocInstantiateInlineWorkflowTemplateOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocUpdateClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)

from astronomer.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperatorAsync,
)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "astronomer-airflow-providers")
CLUSTER_NAME = os.getenv("GCP_DATAPROC_CLUSTER_NAME", "example-cluster-astronomer-providers")
REGION = os.getenv("GCP_LOCATION", "us-central1")
ZONE = os.getenv("GCP_REGION", "us-central1-a")
BUCKET = os.getenv("GCP_DATAPROC_BUCKET", "dataproc-system-tests-astronomer-providers")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
OUTPUT_FOLDER = "wordcount"
OUTPUT_PATH = f"gs://{BUCKET}/{OUTPUT_FOLDER}/"

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]

# Cluster definition: Generating Cluster Config for DataprocCreateClusterOperator
# [START how_to_cloud_dataproc_create_cluster_generate_cluster_config]
path = "gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id="test",
    zone="us-central1-a",
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",
    num_workers=2,
    storage_bucket="test",
    init_actions_uris=[path],
    metadata={"PIP_PACKAGES": "pyyaml requests pandas openpyxl"},
).make()

create_cluster_operator = DataprocCreateClusterOperator(
    task_id="create_dataproc_cluster",
    cluster_name="test",
    project_id="test",
    region="us-central1",
    cluster_config=CLUSTER_GENERATOR_CONFIG,
)
# [END how_to_cloud_dataproc_create_cluster_generate_cluster_config]

# Update options
# [START how_to_cloud_dataproc_updatemask_cluster_operator]
CLUSTER_UPDATE = {
    "config": {"worker_config": {"num_instances": 3}, "secondary_worker_config": {"num_instances": 3}}
}
UPDATE_MASK = {
    "paths": ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]
}
# [END how_to_cloud_dataproc_updatemask_cluster_operator]

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
}


with models.DAG(
    dag_id="example_gcp_dataproc",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "dataproc"],
) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    # [START howto_create_bucket_task]
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket1",
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

    # [START how_to_cloud_dataproc_update_cluster_operator]
    scale_cluster = DataprocUpdateClusterOperator(
        task_id="scale_cluster",
        cluster_name=CLUSTER_NAME,
        cluster=CLUSTER_UPDATE,
        update_mask=UPDATE_MASK,
        graceful_decommission_timeout=TIMEOUT,
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_update_cluster_operator]

    # [START how_to_cloud_dataproc_create_workflow_template]
    create_workflow_template = DataprocCreateWorkflowTemplateOperator(
        task_id="create_workflow_template",
        template=WORKFLOW_TEMPLATE,
        project_id=PROJECT_ID,
        region=REGION,
    )
    # [END how_to_cloud_dataproc_create_workflow_template]

    # [START how_to_cloud_dataproc_trigger_workflow_template]
    trigger_workflow = DataprocInstantiateWorkflowTemplateOperator(
        task_id="trigger_workflow", region=REGION, project_id=PROJECT_ID, template_id=WORKFLOW_NAME
    )
    # [END how_to_cloud_dataproc_trigger_workflow_template]

    # [START how_to_cloud_dataproc_instantiate_inline_workflow_template]
    instantiate_inline_workflow_template = DataprocInstantiateInlineWorkflowTemplateOperator(
        task_id="instantiate_inline_workflow_template", template=WORKFLOW_TEMPLATE, region=REGION
    )
    # [END how_to_cloud_dataproc_instantiate_inline_workflow_template]

    # [START howto_DataprocSubmitJobOperatorAsync]
    pig_task = DataprocSubmitJobOperatorAsync(
        task_id="pig_task", job=PIG_JOB, region=REGION, project_id=PROJECT_ID
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START howto_DataprocSubmitJobOperatorAsync]
    spark_sql_task = DataprocSubmitJobOperatorAsync(
        task_id="spark_sql_task", job=SPARK_SQL_JOB, region=REGION, project_id=PROJECT_ID
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START howto_DataprocSubmitJobOperatorAsync]
    spark_task = DataprocSubmitJobOperatorAsync(
        task_id="spark_task", job=SPARK_JOB, region=REGION, project_id=PROJECT_ID
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START howto_DataprocSubmitJobOperatorAsync]
    hive_task = DataprocSubmitJobOperatorAsync(
        task_id="hive_task", job=HIVE_JOB, region=REGION, project_id=PROJECT_ID
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START howto_DataprocSubmitJobOperatorAsync]
    hadoop_task = DataprocSubmitJobOperatorAsync(
        task_id="hadoop_task", job=HADOOP_JOB, region=REGION, project_id=PROJECT_ID
    )
    # [END howto_DataprocSubmitJobOperatorAsync]
    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]
    # [START howto_delete_buckettask]
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET,
    )
    # [END howto_delete_buckettask]

    create_cluster >> scale_cluster >> create_bucket
    scale_cluster >> create_workflow_template >> trigger_workflow >> delete_cluster
    scale_cluster >> hive_task >> delete_cluster >> delete_bucket
    scale_cluster >> pig_task >> delete_cluster >> delete_bucket
    scale_cluster >> spark_sql_task >> delete_cluster >> delete_bucket
    scale_cluster >> spark_task >> delete_cluster >> delete_bucket
    scale_cluster >> hadoop_task >> delete_cluster >> delete_bucket
