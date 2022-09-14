import json
import os
import uuid
from typing import Any, Union

import pandas as pd
from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class GCSXComBackend(BaseXCom):
    """
    Custom XCom persistence class extends base to support various datatypes.
    Add the below line in the environment
    AIRFLOW__CORE__XCOM_BACKEND=
    astronomer.providers.google.cloud.xcom_backends.gcs.GCSXComBackend
    """

    PREFIX = os.getenv("PREFIX", "GCSXCOM_")
    GCP_CONN_ID = os.getenv("CONNECTION_NAME", "google_cloud_default")
    BUCKET_NAME = os.getenv("XCOM_BACKEND_BUCKET_NAME", "some_bucket_name")

    @staticmethod
    def write_and_upload_value(value: Any) -> str:
        """Convert to string and upload to GCS"""
        key_str = GCSXComBackend.PREFIX + str(uuid.uuid4())
        hook = GCSHook(gcp_conn_id=GCSXComBackend.GCP_CONN_ID)
        if isinstance(value, list):
            value = str(value)
        elif isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, pd.DataFrame):
            value = value.to_json()
        hook.upload(GCSXComBackend.BUCKET_NAME, key_str, data=value)
        return key_str

    @staticmethod
    def read_value(filename: str) -> Union[str, bytes]:
        """Download the file from GCS"""
        # Here we download the file from GCS
        hook = GCSHook(gcp_conn_id=GCSXComBackend.GCP_CONN_ID)
        data = hook.download(GCSXComBackend.BUCKET_NAME, filename)
        return data

    @staticmethod
    def serialize_value(value: Any) -> Any:  # type: ignore[override]
        """Custom XCOM for GCS to serialize the data"""
        value = GCSXComBackend.write_and_upload_value(value)
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        """Custom XCOM for GCS to deserialize the data"""
        result = BaseXCom.deserialize_value(result)
        # Check if result is string and has the configured XCom prefix
        if isinstance(result, str) and result.startswith(GCSXComBackend.PREFIX):
            return GCSXComBackend.read_value(result)
        return result

    def orm_deserialize_value(self) -> str:
        """
        Deserialize amethod which is used to reconstruct ORM XCom object.
        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom ORM model.
        """
        return f"XCOM is uploaded into GCS bucket: {GCSXComBackend.BUCKET_NAME}"
