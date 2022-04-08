"""
This is an example DAG which uses the LivyOperatorAsync.
The tasks below trigger the computation of pi on the Spark instance
using the Java and Python executables provided in the example library.
"""
import os
from datetime import datetime

from airflow import DAG

from astronomer.providers.apache.livy.operators.livy import LivyOperatorAsync

LIVY_JAVA_FILE = os.environ.get("LIVY_JAVA_FILE", "/spark-examples.jar")
LIVY_PYTHON_FILE = os.environ.get("LIVY_PYTHON_FILE", "/user/hadoop/pi.py")

with DAG(
    dag_id="example_livy_operator",
    default_args={"args": [10]},
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "async", "deferrable", "LivyOperatorAsync"],
) as dag:

    # [START create_livy]
    livy_java_task = LivyOperatorAsync(
        task_id="pi_java_task",
        file=LIVY_JAVA_FILE,
        num_executors=1,
        conf={
            "spark.shuffle.compress": "false",
        },
        class_name="org.apache.spark.examples.SparkPi",
        polling_interval=0,
    )

    livy_python_task = LivyOperatorAsync(task_id="pi_python_task", file=LIVY_PYTHON_FILE, polling_interval=30)

    livy_java_task >> livy_python_task
    # [END create_livy]
