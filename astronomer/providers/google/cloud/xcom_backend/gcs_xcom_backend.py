import json
import os
import uuid
from typing import Any, Union

import pandas as pd
from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class CustomXcomGCS(BaseXCom):
    """
    Custom XCOM persistence class extends base to support various datatypes.
    Add the below line in the environment
    AIRFLOW__CORE__XCOM_BACKEND:
    astronomer.providers.google.cloud.xcom_backend.gcs_xcom_backend.py.CustomXcomGCS
    """

    PREFIX = os.getenv("PREFIX", "GCSXCOM_")
    gcp_conn_id = os.getenv("CONNECTION_NAME", "google_cloud_default")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "rajath_xocm_testing")

    @staticmethod
    def write_and_upload_value(value: Any) -> str:
        """Convert to string and upload to GCS"""
        key_str = CustomXcomGCS.PREFIX + str(uuid.uuid4())
        hook = GCSHook(gcp_conn_id=CustomXcomGCS.gcp_conn_id)
        if isinstance(value, list):
            value = str(value)
        elif isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, pd.DataFrame):
            value = value.to_json()
        hook.upload(CustomXcomGCS.BUCKET_NAME, key_str, data=value)
        return key_str

    @staticmethod
    def read_value(filename: str) -> Union[str, bytes]:
        """Downalod the file from GCS"""
        # Here we download the file from GCS
        hook = GCSHook(gcp_conn_id=CustomXcomGCS.gcp_conn_id)
        data = hook.download(CustomXcomGCS.BUCKET_NAME, filename)
        return data

    @staticmethod
    def serialize_value(value: Any) -> Any:  # type: ignore[override]
        """Custom XCOM for GCS to serialize the data"""
        value = CustomXcomGCS.write_and_upload_value(value)
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        """Custom XCOM for GCS to deserialize the data"""
        result = BaseXCom.deserialize_value(result)
        # Check if result is string and have XCOM as prefix
        if isinstance(result, str) and result.startswith(CustomXcomGCS.PREFIX):
            return CustomXcomGCS.read_value(result)
        return result

    def orm_deserialize_value(self) -> str:
        """
        Deserialize amethod which is used to reconstruct ORM XCom object.
        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom orm model.
        """
        return "XCOM uploaded to GCS"
