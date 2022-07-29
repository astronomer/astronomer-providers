import os
from datetime import timedelta

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.timezone import datetime

REGION = os.getenv("GCP_LOCATION", "us-central1")
GCP_PROJECT_NAME = os.getenv("GCP_PROJECT_NAME", "astronomer-airflow-providers")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def gcloud_nuke_callable() -> None:
    """Deletes stale GCP resources."""
    import json
    import logging
    import subprocess
    import tempfile

    sa_json = Variable.get("gcp_nuke_sa_json", deserialize_json=True)

    with tempfile.NamedTemporaryFile("w+") as temp_file:
        json.dump(sa_json, temp_file)
        temp_file.flush()
        sa_file_name = temp_file.name

        # Authenticate with GCP using service account file.
        subprocess.getoutput(f"gcloud auth activate-service-account --key-file={sa_file_name}")
        subprocess.getoutput(f"gcloud config set project {GCP_PROJECT_NAME}")

        # Delete GKE clusters.
        gke_clusters = json.loads(subprocess.getoutput("gcloud beta container clusters list --format=json"))
        for cluster in gke_clusters:
            cluster_link = cluster["selfLink"]
            logging.info("Deleting GKE cluster %s", cluster_link)
            subprocess.getoutput(
                f"echo 'Y' | gcloud beta container clusters delete {cluster_link} --region={REGION}"
            )

        # Delete Dataproc clusters.
        subprocess.getoutput(f"gcloud config set dataproc/region {REGION}")
        dataproc_clusters = json.loads(subprocess.getoutput("gcloud dataproc clusters list --format=json"))
        for cluster in dataproc_clusters:
            cluster_name = cluster["clusterName"]
            logging.info("Deleting Dataproc cluster %s", cluster_name)
            subprocess.getoutput(f"echo 'Y' | gcloud dataproc clusters delete {cluster_name}")

        # Delete compute instances.
        compute_instances = json.loads(subprocess.getoutput("gcloud compute instances list --format=json"))
        for instance in compute_instances:
            instance_name = instance["name"]
            logging.info("Deleting compute instance %s", instance_name)
            subprocess.getoutput(f"echo 'Y' | gcloud compute instances delete {instance_name}")

        # Delete storage buckets.
        buckets_ls = subprocess.getoutput("gcloud alpha storage ls")
        if buckets_ls != "ERROR: (gcloud.alpha.storage.ls) One or more URLs matched no objects.":
            buckets = buckets_ls.split("\n")
            for bucket in buckets:
                logging.info("Deleting bucket %s", bucket)
                subprocess.getoutput(f"gcloud alpha storage rm --recursive {bucket}")


with DAG(
    dag_id="example_gcp_nuke",
    start_date=datetime(2022, 1, 1),
    schedule_interval="30 20 * * *",
    catchup=False,
    default_args=default_args,
    tags=["example", "gcp", "nuke"],
    is_paused_upon_creation=False,
) as dag:
    gcloud_nuke = PythonOperator(task_id="gcloud_nuke", python_callable=gcloud_nuke_callable)
