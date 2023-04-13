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

from astronomer.providers.amazon.aws.xcom_backends.s3 import (
    S3XComBackend,
    _S3XComBackend,
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


class TestS3XComBackend:
    @mock.patch("astronomer.providers.amazon.aws.xcom_backends.s3._S3XComBackend.write_and_upload_value")
    def test_serialize(self, mock_write):
        """
        Asserts that custom xcom is serialized or not
        """
        real_job_id = "12345_hash"
        mock_write.return_value = real_job_id
        result = S3XComBackend.serialize_value(real_job_id)
        assert result == json.dumps(real_job_id).encode("UTF-8")

    @pytest.mark.parametrize(
        "job_id",
        ["1234567890", {"a": "b"}, ["123"]],
    )
    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string")
    def test_write_and_upload(self, mock_upload, mock_uuid, job_id):
        """
        Asserts that custom xcom is uploaded and returns the key
        """
        mock_uuid.return_value = "12345667890"
        result = _S3XComBackend().write_and_upload_value(job_id)
        assert result == "s3_xcom_" + "12345667890"

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    @pytest.mark.parametrize(
        "job_id",
        ["1234567890", {"a": "b"}, ["123"]],
    )
    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string")
    def test_write_and_upload_pickle(self, mock_upload, mock_uuid, job_id):
        """
        Asserts that custom xcom pickle data is uploaded and returns the key
        """
        mock_uuid.return_value = "12345667890"
        result = _S3XComBackend().write_and_upload_value(job_id)
        assert result == "s3_xcom_" + "12345667890"

    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string")
    def test_write_and_upload_pandas(self, mock_upload, mock_uuid):
        """
        Asserts that custom xcom pandas data is uploaded and returns the key
        """
        mock_uuid.return_value = "12345667890"
        result = _S3XComBackend().write_and_upload_value(pd.DataFrame({"numbers": [1], "colors": ["red"]}))
        assert result == "s3_xcom_" + "12345667890" + "_dataframe"

    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string")
    def test_write_and_upload_datetime(self, mock_upload, mock_uuid):
        """
        Asserts that custom xcom datetime object is uploaded and returns the key
        """
        mock_uuid.return_value = "12345667890"
        result = _S3XComBackend().write_and_upload_value(datetime.now())
        assert result == "s3_xcom_" + "12345667890" + "_datetime"

    @pytest.mark.parametrize(
        "job_id",
        ["1234567890", {"a": "b"}, ["123"]],
    )
    @mock.patch("uuid.uuid4")
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string")
    def test_write_and_upload_as_gzip(self, mock_upload, mock_uuid, job_id):
        """
        Asserts that custom xcom as gzip is uploaded and returns the key
        """
        mock_uuid.return_value = "12345667890"
        _S3XComBackend.UPLOAD_CONTENT_AS_GZIP = mock.patch.dict(
            os.environ, {"XCOM_BACKEND_UPLOAD_CONTENT_AS_GZIP": True}, clear=True
        )
        result = _S3XComBackend().write_and_upload_value(job_id)
        assert result == "s3_xcom_" + "12345667890.gz"

    @pytest.mark.parametrize(
        "job_id",
        ["s3_xcom__1234"],
    )
    @mock.patch("astronomer.providers.amazon.aws.xcom_backends.s3._S3XComBackend.download_and_read_value")
    def test_deserialize(self, mock_download, job_id):
        """
        Asserts that custom xcom is deserialized and check for data
        """

        mock_download.return_value = job_id
        real_job_id = BaseXCom(value=json.dumps(job_id).encode("UTF-8"))
        result = S3XComBackend.deserialize_value(real_job_id)
        assert result == job_id

    @pytest.mark.parametrize(
        "job_id",
        ["gcs_xcom_1234"],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
    @mock.patch("builtins.open", create=True)
    def test_download_and_read_value(self, mock_open, mock_download, job_id):
        """
        Asserts that custom xcom is read the data and validate it.
        """
        mock_open.side_effect = [mock.mock_open(read_data=json.dumps(job_id)).return_value]
        mock_download.return_value = job_id
        result = _S3XComBackend().download_and_read_value(job_id)
        assert result == job_id

    @pytest.mark.parametrize(
        "job_id, mock_df_data",
        [
            ("s3_xcom_1234_dataframe", pd.DataFrame({"numbers": [1], "colors": ["red"]}).to_json()),
            (
                "s3_xcom_1234_dataframe",
                bytes(pd.DataFrame({"numbers": [1], "colors": ["red"]}).to_json(), "utf-8"),
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
    @mock.patch("builtins.open", create=True)
    def test_download_and_read_value_pandas(self, mock_open, mock_download, job_id, mock_df_data):
        """
        Asserts that custom xcom is read the pandas data and validate it.
        """
        mock_open.side_effect = [mock.mock_open(read_data=mock_df_data).return_value]
        mock_download.return_value = job_id
        result = _S3XComBackend().download_and_read_value(job_id)
        assert_frame_equal(result, pd.DataFrame({"numbers": [1], "colors": ["red"]}))

    @pytest.mark.parametrize(
        "job_id",
        ["s3_xcom_1234_datetime"],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
    @mock.patch("builtins.open", create=True)
    def test_download_and_read_value_datetime(self, mock_open, mock_download, job_id):
        """
        Asserts that custom xcom is read the datetime object and validate it.
        """
        time = datetime.now()
        mock_open.side_effect = [mock.mock_open(read_data=time.isoformat()).return_value]
        mock_download.return_value = job_id
        result = _S3XComBackend().download_and_read_value(job_id)
        assert result == time

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    @pytest.mark.parametrize(
        "job_id",
        ["s3_xcom_1234"],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
    @mock.patch("builtins.open", create=True)
    def test_download_and_read_value_pickle(self, mock_open, mock_download, job_id):
        """
        Asserts that custom xcom is read the pickle data and validate it.
        """
        mock_open.side_effect = [mock.mock_open(read_data=pickle.dumps(job_id)).return_value]
        mock_download.return_value = job_id
        result = _S3XComBackend().download_and_read_value(job_id)
        assert result == job_id

    @pytest.mark.parametrize(
        "job_id",
        ["s3_xcom_1234"],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
    @mock.patch("builtins.open", create=True)
    def test_custom_xcom_s3_download_and_read_value_bytes(self, mock_open, mock_download, job_id):
        """
        Asserts that custom xcom is read the bytes data and validate it.
        """
        mock_open.side_effect = [mock.mock_open(read_data=b'{ "Class": "Email addresses"}').return_value]
        mock_download.return_value = job_id
        result = _S3XComBackend().download_and_read_value(job_id)
        assert result == {"Class": "Email addresses"}

    @pytest.mark.parametrize(
        "job_id",
        ["s3_xcom_1234.gz"],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
    @mock.patch("builtins.open", create=True)
    def test_download_and_read_value_gzip(self, mock_open, mock_download, job_id):
        """
        Asserts that custom xcom is gzip content and validate it.
        """
        mock_open.side_effect = [
            mock.mock_open(read_data=gzip.compress(b'{"Class": "Email addresses"}')).return_value
        ]
        mock_download.return_value = job_id
        result = _S3XComBackend().download_and_read_value(job_id)
        assert result == {"Class": "Email addresses"}

    def test_orm_deserialize_value(self):
        """
        Asserts that custom xcom has called the orm deserialized
        value method and check for data.
        """
        result = S3XComBackend().orm_deserialize_value()
        assert result == "XCOM is uploaded into S3 bucket: airflow_xcom_backend_default_bucket"
