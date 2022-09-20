from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection
from kubernetes_asyncio import client

from astronomer.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHookAsync
from tests.utils.airflow_util import get_conn


class TestKubernetesHookAsync:
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
        self,
        mock_config,
        in_cluster,
        config_file,
        cluster_context,
    ):
        """
        Asserts that when in_cluster set true and false then correct kube configuration method awaited.
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
    async def test_get_api_client_async(self, mock__load_config):
        """Assert that get_api_client_async return correct client type"""
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
    async def test_load_config_with_conn_id(self, mock_get_connection, load_kube_config):
        """Assert that kube client get loaded from Airflow connection"""
        hook = KubernetesHookAsync(conn_id="test_conn")
        mock_get_connection.return_value = get_conn()
        await hook._load_config()
        load_kube_config.assert_awaited()

    @pytest.mark.asyncio
    @mock.patch("kubernetes_asyncio.config.load_incluster_config")
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    async def test_load_config_with_in_cluster(self, mock_get_connection, load_incluster_config):
        """Assert that kube client get loaded from in cluster config"""
        hook = KubernetesHookAsync(in_cluster=True)
        mock_get_connection.return_value = Connection(
            conn_id="test_conn",
            extra={"kubernetes": {"in_cluster": True}},
        )
        await hook._load_config()
        load_incluster_config.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    async def test_load_config_with_more_than_one_config(self, mock_get_connection):
        """Assert that raise exception if more than on config provided"""
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
    async def test_load_config_with_kube_config_path(self, mock_get_connection, mock_load_kube_config):
        """Assert that raise config get loaded from kube config file path"""
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
    async def test_load_config_with_kube_config(self, mock_get_connection, mock_load_kube_config):
        """Assert that raise config get loaded from kube config"""
        hook = KubernetesHookAsync(in_cluster=False)
        mock_get_connection.return_value = Connection(
            conn_id="test_conn",
            extra={"extra__kubernetes__kube_config": "config_file"},
        )
        mock_load_kube_config.return_value = {}
        actual = await hook._load_config()
        mock_load_kube_config.assert_awaited()
        assert isinstance(actual, client.ApiClient)
