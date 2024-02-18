from __future__ import annotations

import warnings
from typing import Any

import aiofiles
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes_asyncio import client, config


def get_field(extras: dict[str, Any], field_name: str, strict: bool = False) -> Any:
    """Get field from extra, first checking short name, then for backward compatibility we check for prefixed name."""
    backward_compatibility_prefix = "extra__kubernetes__"
    if field_name.startswith("extra__"):
        raise ValueError(
            f"Got prefixed name {field_name}; please remove the '{backward_compatibility_prefix}' prefix "
            "when using this method."
        )
    if field_name in extras:
        return extras[field_name] or None
    prefixed_name = f"{backward_compatibility_prefix}{field_name}"
    if prefixed_name in extras:
        return extras[prefixed_name] or None
    if strict:
        raise KeyError(f"Field {field_name} not found in extras")


class KubernetesHookAsync(KubernetesHook):  # noqa: D101
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook` instead
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use `airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook`"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)

    async def _load_config(self) -> client.ApiClient:
        """
        cluster_context: Optional[str] = None,
        config_file: Optional[str] = None,
        in_cluster: Optional[bool] = None,

        """
        if self.conn_id:
            connection = self.get_connection(self.conn_id)
            extras = connection.extra_dejson
        else:
            extras = {}
        in_cluster = self._coalesce_param(self.in_cluster, get_field(extras, "in_cluster"))
        cluster_context = self._coalesce_param(self.cluster_context, get_field(extras, "cluster_context"))
        kubeconfig_path = self._coalesce_param(self.config_file, get_field(extras, "kube_config_path"))
        kubeconfig = get_field(extras, "kube_config") or None
        num_selected_configuration = len([o for o in [in_cluster, kubeconfig, kubeconfig_path] if o])

        if num_selected_configuration > 1:
            raise AirflowException(
                "Invalid connection configuration. Options kube_config_path, "
                "kube_config, in_cluster are mutually exclusive. "
                "You can only use one option at a time."
            )
        if in_cluster:
            self.log.debug("loading kube_config from: in_cluster configuration")
            config.load_incluster_config()
            return client.ApiClient()

        if kubeconfig_path is not None:
            self.log.debug("loading kube_config from: %s", kubeconfig_path)
            await config.load_kube_config(
                config_file=kubeconfig_path,
                client_configuration=self.client_configuration,
                context=cluster_context,
            )
            return client.ApiClient()

        if kubeconfig is not None:
            async with aiofiles.tempfile.NamedTemporaryFile() as temp_config:
                self.log.debug("loading kube_config from: connection kube_config")
                await temp_config.write(kubeconfig.encode())
                await temp_config.flush()
                await config.load_kube_config(
                    config_file=temp_config.name,
                    client_configuration=self.client_configuration,
                    context=cluster_context,
                )
            return client.ApiClient()

        self.log.debug("loading kube_config from: default file")
        await config.load_kube_config(
            client_configuration=self.client_configuration,
            context=cluster_context,
        )

    async def get_api_client_async(self) -> client.ApiClient:
        """Create an API Client object to interact with Kubernetes"""
        kube_client = await self._load_config()
        if kube_client is not None:
            return kube_client
        return client.ApiClient()
