import json
from unittest import mock

import pandas as pd
import pytest
from airflow.models.xcom import BaseXCom

from astronomer.providers.amazon.aws.xcom_backends.s3 import S3XComBackend


@mock.patch("astronomer.providers.amazon.aws.xcom_backends.s3.S3XComBackend.write_and_upload_value")
def test_custom_xcom_s3_serialize(mock_write):
    """
    Asserts that custom xcom is serialize or not
    """
    real_job_id = "12345_hash"
    mock_write.return_value = real_job_id
    result = S3XComBackend.serialize_value(real_job_id)
    assert result == json.dumps(real_job_id).encode("UTF-8")


@pytest.mark.parametrize(
    "job_id",
    ["1234567890", {"a": "b"}, ["123"], pd.DataFrame({"numbers": [1], "colors": ["red"]})],
)
@mock.patch("uuid.uuid4")
@mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string")
def test_custom_xcom_s3_write_and_upload(mock_upload, mock_uuid, job_id):
    """
    Asserts that custom xcom is upload and returns key
    """
    mock_uuid.return_value = "12345667890"
    result = S3XComBackend.write_and_upload_value(job_id)
    assert result == "s3_xcom_" + "12345667890"


@pytest.mark.parametrize(
    "job_id",
    ["s3_xcom__1234", "1234"],
)
@mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file")
def test_custom_xcom_s3_deserialize(mock_download, job_id):
    """
    Asserts that custom xcom is deserialized and check for data
    """
    mock_download.return_value = job_id
    real_job_id = BaseXCom(value=json.dumps(job_id).encode("UTF-8"))
    result = S3XComBackend.deserialize_value(real_job_id)
    assert result == job_id


def test_custom_xcom_s3_orm_deserialize_value():
    """
    Asserts that custom xcom has called the orm deserialized
    value method and check for data.
    """
    result = S3XComBackend().orm_deserialize_value()
    assert result == "XCOM is uploaded into S3 bucket: airflow_xcom_backend_default_bucket"
