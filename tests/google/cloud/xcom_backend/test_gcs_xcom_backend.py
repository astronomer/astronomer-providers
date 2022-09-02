import json
from unittest import mock

import pandas as pd
import pytest
from airflow.models.xcom import BaseXCom

from astronomer.providers.google.cloud.xcom_backend.gcs_xcom_backend import (
    CustomXcomGCS,
)


@mock.patch(
    "astronomer.providers.google.cloud.xcom_backend.gcs_xcom_backend.CustomXcomGCS.write_and_upload_value"
)
def test_custom_xcom_gcs_serialize(mock_write):
    """
    Asserts that custom xcom is serialize or not
    """
    real_job_id = "12345_hash"
    mock_write.return_value = real_job_id
    result = CustomXcomGCS.serialize_value(real_job_id)
    assert result == json.dumps(real_job_id).encode("UTF-8")


@pytest.mark.parametrize(
    "job_id",
    ["1234567890", {"a": "b"}, ["123"], pd.DataFrame({"numbers": [1], "colors": ["red"]})],
)
@mock.patch("uuid.uuid4")
@mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.upload")
def test_custom_xcom_gcs_write_and_upload(mock_upload, mock_uuid, job_id):
    """
    Asserts that custom xcom is upload and returns key
    """
    mock_uuid.return_value = "12345667890"
    result = CustomXcomGCS.write_and_upload_value(job_id)
    assert result == "GCSXCOM_" + "12345667890"


@pytest.mark.parametrize(
    "job_id",
    ["GCSXCOM_1234", "1234"],
)
@mock.patch("airflow.providers.google.cloud.hooks.gcs.GCSHook.download")
def test_custom_xcom_gcs_deserialize(mock_download, job_id):
    """
    Asserts that custom xcom is deserialized and check for data
    """
    mock_download.return_value = job_id
    real_job_id = BaseXCom(value=json.dumps(job_id).encode("UTF-8"))
    result = CustomXcomGCS.deserialize_value(real_job_id)
    assert result == job_id


def test_custom_xcom_gcs_orm_deserialize_value():
    """
    Asserts that custom xcom has called the orm deserialized
    value method and check for data.
    """

    result = CustomXcomGCS().orm_deserialize_value()
    assert result == "XCOM uploaded to GCS"
