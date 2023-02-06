from __future__ import annotations

import contextlib
import gzip
import json
import os
import pickle  # nosec
from datetime import datetime
from unittest import mock

import pandas as pd
import pytest
from airflow import settings
from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from pandas.util.testing import assert_frame_equal

from astronomer.providers.google.cloud.xcom_backends.gcs import (
    GCSXComBackend,
    _GCSXComBackend,
)


@contextlib.contextmanager
def conf_vars(overrides):
    original = {}
    original_env_vars = {}
    for (section, key), value in overrides.items():
        env = conf._env_var_name(section, key)
        if env in os.environ:
            original_env_vars[env] = os.environ.pop(env)

        if conf.has_option(section, key):
            original[(section, key)] = conf.get(section, key)
        else:
            original[(section, key)] = None
        if value is not None:
            if not conf.has_section(section):
                conf.add_section(section)
            conf.set(section, key, value)
        else:
            conf.remove_option(section, key)
    settings.configure_vars()
    try:
        yield
    finally:
        for (section, key), value in original.items():
            if value is not None:
                conf.set(section, key, value)
            else:
                conf.remove_option(section, key)
        for env, value in original_env_vars.items():
            os.environ[env] = value
        settings.configure_vars()


class TestGCSXComBackend:
    @mock.patch("astronomer.providers.google.cloud.xcom_backends.gcs._GCSXComBackend.write_and_upload_value")
    def test_custom_xcom_gcs_serialize(self, mock_write):
        """Asserts that custom xcom is serialize or not"""
        real_job_id = "12345_hash"
        mock_write.return_value = real_job_id
        result = GCSXComBackend.serialize_value(real_job_id)
        assert result == json.dumps(real_job_id).encode("UTF-8")

    @pytest.mark.parametrize(
        "job_id",
        ["1234567890", {"a": "b"}, ["123"]],
    )
    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
    def test_custom_xcom_gcs_write_and_upload(self, mock_upload, mock_uuid, job_id):
        """Asserts that custom xcom is upload and returns key"""
        mock_uuid.return_value = "12345667890"
        result = _GCSXComBackend().write_and_upload_value(job_id)
        assert result == "gcs_xcom_" + "12345667890"

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    @pytest.mark.parametrize(
        "job_id",
        ["1234567890", {"a": "b"}, ["123"]],
    )
    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
    def test_custom_xcom_gcs_write_and_upload_pickle(self, mock_upload, mock_uuid, job_id):
        """Asserts that custom xcom is upload and returns key"""
        mock_uuid.return_value = "12345667890"
        result = _GCSXComBackend().write_and_upload_value(job_id)
        assert result == "gcs_xcom_" + "12345667890"

    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
    def test_custom_xcom_gcs_write_and_upload_pandas(self, mock_upload, mock_uuid):
        """Asserts that custom xcom is upload and returns key"""
        mock_uuid.return_value = "12345667890"
        result = _GCSXComBackend().write_and_upload_value(pd.DataFrame({"numbers": [1], "colors": ["red"]}))
        assert result == "gcs_xcom_" + "12345667890" + "_dataframe"

    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
    def test_custom_xcom_gcs_write_and_upload_datetime(self, mock_upload, mock_uuid):
        """Asserts that custom xcom is upload and returns key"""
        mock_uuid.return_value = "12345667890"
        result = _GCSXComBackend().write_and_upload_value(datetime.now())
        assert result == "gcs_xcom_" + "12345667890" + "_datetime"

    @pytest.mark.parametrize(
        "job_id",
        ["1234567890", {"a": "b"}, ["123"]],
    )
    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
    def test_custom_xcom_gcs_write_and_upload_as_gzip(self, mock_upload, mock_uuid, job_id):
        """Asserts that custom xcom is upload and returns key"""
        mock_uuid.return_value = "12345667890"
        _GCSXComBackend.UPLOAD_CONTENT_AS_GZIP = mock.patch.dict(
            os.environ, {"XCOM_BACKEND_UPLOAD_CONTENT_AS_GZIP": True}, clear=True
        )
        result = _GCSXComBackend().write_and_upload_value(job_id)
        assert result == "gcs_xcom_" + "12345667890.gz"

    @pytest.mark.parametrize(
        "job_id",
        ["gcs_xcom_1234"],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.download")
    def test_custom_xcom_gcs_deserialize(self, mock_download, job_id):
        """Asserts that custom xcom is deserialized and check for data"""
        mock_download.return_value = json.dumps(job_id).encode("UTF-8")
        real_job_id = BaseXCom(value=json.dumps(job_id).encode("UTF-8"))
        result = GCSXComBackend.deserialize_value(real_job_id)
        assert result == job_id

    @pytest.mark.parametrize(
        "job_id",
        ["gcs_xcom_1234_dataframe"],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.download")
    def test_custom_xcom_gcs_deserialize_pandas(self, mock_read_value, job_id):
        """Asserts that custom xcom is deserialized and check for data"""
        mock_read_value.return_value = pd.DataFrame({"numbers": [1], "colors": ["red"]}).to_json()
        result = _GCSXComBackend.download_and_read_value(job_id)
        assert_frame_equal(result, pd.DataFrame({"numbers": [1], "colors": ["red"]}))

    @pytest.mark.parametrize(
        "job_id",
        ["gcs_xcom_1234_datetime"],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.download")
    def test_custom_xcom_gcs_deserialize_datetime(self, mock_read_value, job_id):
        """Asserts that custom xcom is deserialized and check for data"""
        time = datetime.now()
        mock_read_value.return_value = time.isoformat()
        result = _GCSXComBackend.download_and_read_value(job_id)
        assert result == time

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    @pytest.mark.parametrize(
        "job_id",
        ["gcs_xcom_1234"],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.download")
    def test_custom_xcom_gcs_deserialize_pickle(self, mock_download, job_id):
        """Asserts that custom xcom is deserialized and check for data"""
        mock_download.return_value = pickle.dumps(job_id)
        result = _GCSXComBackend.download_and_read_value(job_id)
        assert result == job_id

    @pytest.mark.parametrize(
        "job_id",
        ["gcs_xcom_1234"],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.download")
    def test_custom_xcom_gcs_deserialize_bytes(self, mock_download, job_id):
        """Asserts that custom xcom is deserialized and check for data"""
        mock_download.return_value = b'{ "Class": "Email addresses"}'
        result = _GCSXComBackend.download_and_read_value(job_id)
        assert result == {"Class": "Email addresses"}

    @pytest.mark.parametrize(
        "job_id",
        ["gcs_xcom_1234.gz"],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.download")
    def test_custom_xcom_gcs_deserialize_gzip(self, mock_download, job_id):
        """Asserts that custom xcom is deserialized and check for data"""
        mock_download.return_value = gzip.compress(b'{"Class": "Email addresses"}')
        result = _GCSXComBackend.download_and_read_value(job_id)
        assert result == {"Class": "Email addresses"}

    def test_custom_xcom_gcs_orm_deserialize_value(self):
        """
        Asserts that custom xcom has called the orm deserialized
        value method and check for data.
        """
        result = GCSXComBackend().orm_deserialize_value()
        assert result == "XCOM is uploaded into GCS bucket: airflow_xcom_backend_default_bucket"
