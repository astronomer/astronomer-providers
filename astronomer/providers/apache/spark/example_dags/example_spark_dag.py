"""
Example Airflow DAG to submit Apache Spark applications using
`SparkSubmitOperator`.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator

from astronomer.providers.apache.spark.operators.spark_sumbit import (
    SparkSubmitOperatorAsync,
)

SPARK_CONN_ID = os.environ.get("SPARK_CONN_ID", "spark_default")

with DAG(
    dag_id="example_spark_operator",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "async", "spark"],
) as dag:

    # [START howto_Dummy_operator_start]
    start = DummyOperator(task_id="start")
    # [END howto_Dummy_operator_start]

    # [START howto_operator_spark_submit_async]
    submit_job_sync = SparkSubmitOperatorAsync(
        application="/opt/spark-3.1.3-bin-hadoop3.2/examples/src/main/python/pi.py",
        task_id="submit_job_async",
        conn_id=SPARK_CONN_ID,
    )
    # [END howto_operator_spark_submit_async]

    # [START howto_Dummy_operator_end]
    end = DummyOperator(task_id="end")
    # [END howto_Dummy_operator_end]

    start >> submit_job_sync >> end
