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

from unittest import mock

import pytest

from astronomer_operators.cncf.kubernetes.hooks.kubernetes_async import (
    KubernetesHookAsync,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "in_cluster, config_file, cluster_context",
    [
        (True, None, None),
        (False, "/path/to/file", "my-context"),
    ],
)
@mock.patch("astronomer_operators.cncf.hooks.kubernetes_async.config")
@pytest.mark.xfail
async def test_kubernetes__load_config(
    mock_config,
    in_cluster,
    config_file,
    cluster_context,
):
    """
    Asserts that a run state is returned as expected while a Databricks run
    is in a PENDING state (i.e. "RUNNING") and after it reaches a TERMINATED
    state (i.e. "SUCCESS").
    """
    hook = KubernetesHookAsync(
        in_cluster=in_cluster,
        config_file=config_file,
        cluster_context=cluster_context,
        conn_id=None,
    )
    await hook.get_api_client_async()
    if in_cluster:
        mock_config.load_incluster_config.assert_awaited()
        mock_config.load_kube_config.assert_not_awaited()
    else:
        mock_config.load_incluster_config.assert_awaited()
        mock_config.load_kube_config.assert_awaited_with(
            config_file=config_file,
            client_configuration=None,
            context=cluster_context,
        )
