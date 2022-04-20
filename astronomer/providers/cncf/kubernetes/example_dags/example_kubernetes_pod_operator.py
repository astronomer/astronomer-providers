import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperatorAsync,
)

namespace = conf.get("kubernetes", "NAMESPACE")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

if namespace == "default":
    config_file = None
    in_cluster = False
else:
    in_cluster = True
    config_file = None

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}

with DAG(
    dag_id="example_kubernetes_operator",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "k8s"],
) as dag:
    create_k8s_pod = KubernetesPodOperatorAsync(
        task_id="create_k8s_pod",
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
        name="astro_k8s_test_pod",
        image="ubuntu",
        cmds=["/bin/sh"],
        arguments=["-c", "sleep 30"],
    )

    create_k8s_pod
