from unittest import mock

import pytest
from google.api_core import gapic_v1
from google.auth import credentials as ga_credentials
from google.cloud.dataproc_v1 import ClusterControllerAsyncClient

from astronomer.providers.google.cloud.hooks.dataproc_async import DataprocHookAsync


@mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials")
def test_get_cluster_client(mock_get_credentials):
    """assert that get_cluster_client return ClusterControllerAsyncClient"""
    mock_get_credentials.return_value = ga_credentials.AnonymousCredentials()
    hook = DataprocHookAsync()
    assert isinstance(hook.get_cluster_client(location="us-west"), ClusterControllerAsyncClient)


@pytest.mark.asyncio
@mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials")
@mock.patch("google.cloud.dataproc_v1.ClusterControllerAsyncClient.get_cluster")
async def test_get_cluster(mock_get_cluster, mock_get_cluster_client):
    """assert that get_cluster called with correct param"""
    mock_get_cluster_client.return_value = ga_credentials.AnonymousCredentials()
    hook = DataprocHookAsync()
    await hook.get_cluster(region="us-west", cluster_name="test_cluster", project_id="test_project")

    mock_get_cluster.assert_called_once_with(
        region="us-west",
        cluster_name="test_cluster",
        project_id="test_project",
        retry=gapic_v1.method.DEFAULT,
        metadata=(),
    )
