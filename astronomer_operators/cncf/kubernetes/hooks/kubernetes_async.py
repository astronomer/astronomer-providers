# Copyright 2022 Astronomer Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import aiofiles
from airflow import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes_asyncio import client, config


class KubernetesHookAsync(KubernetesHook):
    async def _load_config(self):
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
        in_cluster = self._coalesce_param(
            self.in_cluster, extras.get("extra__kubernetes__in_cluster") or None
        )
        cluster_context = self._coalesce_param(
            self.cluster_context, extras.get("extra__kubernetes__cluster_context") or None
        )
        kubeconfig_path = self._coalesce_param(
            self.config_file, extras.get("extra__kubernetes__kube_config_path") or None
        )
        kubeconfig = extras.get("extra__kubernetes__kube_config") or None
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
                temp_config.write(kubeconfig.encode())
                temp_config.flush()
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

    async def get_api_client_async(self):
        await self._load_config()
        return client.ApiClient()
