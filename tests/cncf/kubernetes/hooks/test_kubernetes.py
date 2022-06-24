from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection
from kubernetes_asyncio import client

from astronomer.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHookAsync


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "in_cluster, config_file, cluster_context",
    [
        (True, None, None),
        (False, "/path/to/file", "my-context"),
    ],
)
@mock.patch("astronomer.providers.cncf.kubernetes.hooks.kubernetes_async.config")
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


@pytest.mark.asyncio
@mock.patch("astronomer.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHookAsync._load_config")
async def test_get_api_client_async(mock__load_config):
    mock__load_config.return_value = client.ApiClient()

    hook = KubernetesHookAsync(
        in_cluster=True,
        config_file="path/kube/config",
        cluster_context=None,
        conn_id=None,
    )
    kube_client = await hook.get_api_client_async()
    assert isinstance(kube_client, client.ApiClient)


@pytest.mark.asyncio
@mock.patch("kubernetes_asyncio.config.load_kube_config")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
async def test_load_config_with_conn_id(mock_get_connection, load_kube_config):
    hook = KubernetesHookAsync(conn_id="test_conn")
    mock_get_connection.return_value = Connection(
        conn_id="test_conn",
        extra={},
    )
    await hook._load_config()
    load_kube_config.assert_awaited()


@pytest.mark.asyncio
@mock.patch("kubernetes_asyncio.config.load_incluster_config")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
async def test_load_config_with_in_cluster(mock_get_connection, load_incluster_config):
    hook = KubernetesHookAsync(in_cluster=True)
    mock_get_connection.return_value = Connection(
        conn_id="test_conn",
        extra={"kubernetes": {"in_cluster": True}},
    )
    await hook._load_config()
    load_incluster_config.assert_called_once()


@pytest.mark.asyncio
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
async def test_load_config_with_more_than_one_config(mock_get_connection):
    hook = KubernetesHookAsync(in_cluster=True, config_file="kube_config_path")
    mock_get_connection.return_value = Connection(
        conn_id="test_conn",
        extra={"kubernetes": {"in_cluster": True, "kube_config": {}, "kube_config_path": "config_file"}},
    )
    with pytest.raises(AirflowException):
        await hook._load_config()


@pytest.mark.asyncio
@mock.patch("kubernetes_asyncio.config.load_kube_config")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
async def test_load_config_with_kube_config_path(mock_get_connection, mock_load_kube_config):
    hook = KubernetesHookAsync(in_cluster=False)
    mock_get_connection.return_value = Connection(
        conn_id="test_conn",
        extra={"extra__kubernetes__kube_config_path": "config_file"},
    )
    mock_load_kube_config.return_value = {}
    actual = await hook._load_config()
    mock_load_kube_config.assert_awaited()
    assert isinstance(actual, client.ApiClient)


@pytest.mark.asyncio
@mock.patch("kubernetes_asyncio.config.load_kube_config")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
async def test_load_config_with_kube_config(mock_get_connection, mock_load_kube_config):
    hook = KubernetesHookAsync(in_cluster=False)
    mock_get_connection.return_value = Connection(
        conn_id="test_conn",
        extra={"extra__kubernetes__kube_config": "config_file"},
    )
    mock_load_kube_config.return_value = {}
    actual = await hook._load_config()
    mock_load_kube_config.assert_awaited()
    assert isinstance(actual, client.ApiClient)
