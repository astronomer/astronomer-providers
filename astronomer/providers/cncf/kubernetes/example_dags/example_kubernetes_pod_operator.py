from datetime import datetime

from airflow.configuration import conf
from airflow.models.dag import DAG

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperatorAsync,
)

namespace = conf.get("kubernetes", "NAMESPACE")

if namespace == "default":
    config_file = None
    in_cluster = False
else:
    in_cluster = True
    config_file = None


with DAG(
    dag_id="kpo_async",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
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
