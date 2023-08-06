from unittest import mock
from unittest.mock import MagicMock

import pytest
import yaml

from astronomer.providers.google.cloud.gke_utils import (
    _get_impersonation_token,
    _write_token_into_config,
)


@mock.patch("astronomer.providers.google.cloud.gke_utils.ClusterManagerClient")
@mock.patch("astronomer.providers.google.cloud.gke_utils.impersonated_credentials")
def test__get_impersonation_token(mock_impersonated_credentials, mock_cluster_manager_client):
    mock_token = MagicMock()
    mock_impersonated_credentials.Credentials.return_value.token = mock_token
    assert (
        _get_impersonation_token(
            MagicMock(), "impersonation_account", "project_id", "location", "cluster_name"
        )
        == mock_token
    )


@mock.patch("astronomer.providers.google.cloud.gke_utils.ClusterManagerClient")
@mock.patch("astronomer.providers.google.cloud.gke_utils.impersonated_credentials")
def test__get_impersonation_token_with_exception(mock_impersonated_credentials, mock_cluster_manager_client):
    mock_token = MagicMock()
    mock_impersonated_credentials.Credentials.return_value.token = mock_token

    mock_cluster_manager_client.side_effect = Exception
    with pytest.raises(Exception):
        _get_impersonation_token(
            MagicMock(), "impersonation_account", "project_id", "location", "cluster_name"
        )


def test__write_token_into_config(tmp_path):
    fake_config_content = {
        "apiVersion": "v1",
        "clusters": [{"cluster": {}, "name": "user_name"}],
        "contexts": [{"context": {"cluster": "cluster_name", "user": "user_name"}, "name": "cluster_name"}],
        "current-context": "cluster_name",
        "kind": "Config",
        "preferences": {},
        "users": [{"name": "user_name", "user": {}}],
    }
    config_direcrory = tmp_path / ".kube"
    config_direcrory.mkdir()
    config_file = config_direcrory / "config.yaml"

    with open(config_file, "w") as output_file:
        yaml.safe_dump(fake_config_content, output_file)

    _write_token_into_config(config_file, "token", None)

    with open(config_file) as input_file:
        config_content = yaml.safe_load(input_file.read())

    assert config_content["users"][0]["user"]["auth-provider"]["config"]["access-token"] == "token"


def test__write_token_into_config_with_exception(tmp_path):
    config_direcrory = tmp_path / ".kube"
    config_direcrory.mkdir()
    config_file = config_direcrory / "config.yaml"
    config_file.write_text("")
    fake_config_content = {
        "apiVersion": "v1",
        "clusters": [{"cluster": {}, "name": "user_name"}],
        "current-context": "cluster_name",
        "kind": "Config",
        "preferences": {},
        "users": [{"name": "user_name", "user": {}}],
    }
    with open(config_file, "w") as output_file:
        yaml.safe_dump(fake_config_content, output_file)

    _write_token_into_config(config_file, "token", None)

    with open(config_file) as input_file:
        config_content = yaml.safe_load(input_file.read())

    assert config_content == fake_config_content
