"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
"""

import os
from datetime import datetime

from airflow import models

from astronomer.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperatorAsync,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "astronomer-airflow-providers")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "cluster-69de")
REGION = os.environ.get("GCP_LOCATION", "us-central1")
ZONE = os.environ.get("GCP_REGION", "us-central1-a")


SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
}

SPARK_SQL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_sql_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}

with models.DAG(
    "example_gcp_dataproc_rajath",
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    spark_task_async_providers = DataprocSubmitJobOperatorAsync(
        task_id="spark_task_async_providers", job=SPARK_JOB, region=REGION, project_id=PROJECT_ID
    )
    spark_sql_task = DataprocSubmitJobOperatorAsync(
        task_id="spark_sql_task", job=SPARK_SQL_JOB, region=REGION, project_id=PROJECT_ID
    )
    spark_task_async_providers
    spark_sql_task
