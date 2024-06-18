from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from typing import Generator, Sequence

import yaml
from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.process_utils import execute_in_subprocess, patch_environ
from google.auth import impersonated_credentials
from google.auth.credentials import Credentials
from google.cloud.container_v1 import ClusterManagerClient
from kubernetes_asyncio.config.kube_config import KubeConfigLoader

KUBE_CONFIG_ENV_VAR = "KUBECONFIG"


def _get_impersonation_token(
    creds: Credentials,
    impersonation_account: str,
    project_id: str,
    location: str,
    cluster_name: str | None = None,
) -> str:
    impersonated_creds = impersonated_credentials.Credentials(
        source_credentials=creds,
        target_principal=impersonation_account,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    try:
        client = ClusterManagerClient(credentials=impersonated_creds)
        name = f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"
        client.get_cluster(name=name)
    except Exception as e:
        raise Exception(f"Error while creating impersonated creds: {e}") from e
    return str(impersonated_creds.token)


def _write_token_into_config(config_name: str, token: str, cluster_context: str | None = None) -> None:
    with open(config_name) as input_file:
        config_content = yaml.safe_load(input_file.read())

    kube_config_loader = KubeConfigLoader(config_content)
    kube_config_loader.set_active_context(cluster_context)
    kube_config_loader._user.value["auth-provider"] = {
        "name": "gcp",
        "config": {"access-token": token},
    }

    with open(config_name, "w") as output_file:
        yaml.dump(kube_config_loader._config.value, output_file)


@contextmanager
def _get_gke_config_file(
    gcp_conn_id: str,
    project_id: str | None,
    cluster_name: str,
    impersonation_chain: str | Sequence[str] | None,
    regional: bool,
    location: str,
    use_internal_ip: bool,
    cluster_context: str | None = None,
) -> Generator[str, None, None]:  # pragma: no cover
    hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
    project_id = project_id or hook.project_id

    if not project_id:
        raise AirflowException(
            "The project id must be passed either as "
            "keyword project_id parameter or as project_id extra "
            "in Google Cloud connection definition. Both are not set!"
        )

    # Write config to a temp file and set the environment variable to point to it.
    # This is to avoid race conditions of reading/writing a single file
    # fmt: off
    with tempfile.NamedTemporaryFile() as conf_file, patch_environ(
            {KUBE_CONFIG_ENV_VAR: conf_file.name}), hook.provide_authorized_gcloud():
        # fmt: on
        # Attempt to get/update credentials
        # We call gcloud directly instead of using google-cloud-python api
        # because there is no way to write kubernetes config to a file, which is
        # required by KubernetesPodOperator.
        # The gcloud command looks at the env variable `KUBECONFIG` for where to save
        # the kubernetes config file.
        cmd = [
            "gcloud",
            "container",
            "clusters",
            "get-credentials",
            cluster_name,
            "--project",
            project_id,
        ]
        impersonation_account = None
        if impersonation_chain:
            if isinstance(impersonation_chain, str):
                impersonation_account = impersonation_chain
            elif len(impersonation_chain) == 1:
                impersonation_account = impersonation_chain[0]
            else:
                raise AirflowException(
                    "Chained list of accounts is not supported, please specify only one service account"
                )

            cmd.extend(
                [
                    "--impersonate-service-account",
                    impersonation_account,
                ]
            )

        if regional:
            cmd.append("--region")
        else:
            cmd.append("--zone")
        cmd.append(location)
        if use_internal_ip:
            cmd.append("--internal-ip")
        execute_in_subprocess(cmd)

        if impersonation_account:
            token = _get_impersonation_token(
                hook.get_credentials(),
                impersonation_account,
                project_id,
                location,
                cluster_name,
            )
            _write_token_into_config(conf_file.name, token, cluster_context)

        # Tell `KubernetesPodOperator` where the config file is located
        yield os.environ[KUBE_CONFIG_ENV_VAR]
