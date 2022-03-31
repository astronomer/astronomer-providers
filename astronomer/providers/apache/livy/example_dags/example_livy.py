# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
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
