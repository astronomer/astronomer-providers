"""Example DAG demonstrating the usage of the KubernetesPodOperatorAsync."""
from datetime import datetime

from airflow import DAG
from airflow.configuration import conf

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperatorAsync

namespace = conf.get("kubernetes", "NAMESPACE")

if namespace == "default":
    config_file = None
    in_cluster = False
else:
    in_cluster = True
    config_file = None


with DAG(
    dag_id="async_kpo",
    start_date=datetime(2023, 6, 22),
    schedule=None,
    catchup=False,
    tags=["k8s", "core", "async"],
    max_active_runs=1,
) as dag:
    simple_async = KubernetesPodOperatorAsync(
        task_id="simple_async",
        namespace=namespace,
        in_cluster=in_cluster,
        config_file=config_file,
        name="simple_async",
        image="ubuntu",
        cmds=["/bin/sh"],
        arguments=["-c", "sleep 30"],
    )
