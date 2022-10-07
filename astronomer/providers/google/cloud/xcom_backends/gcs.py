import json
import os
import pickle  # nosec
import uuid
from typing import Any

import pandas as pd
from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class GCSXComBackend(BaseXCom):
    """
    Custom XCom persistence class extends base to support various datatypes.
    To use this XCom Backend, add the environment variable `AIRFLOW__CORE__XCOM_BACKEND`
    to your environment and set it to
    `astronomer.providers.google.cloud.xcom_backends.gcs.GCSXComBackend`
    """

    PREFIX = os.getenv("XCOM_BACKEND_PREFIX", "gcs_xcom_")
    GCP_CONN_ID = os.getenv("XCOM_BACKEND_CONNECTION_NAME", "google_cloud_default")
    BUCKET_NAME = os.getenv("XCOM_BACKEND_BUCKET_NAME", "airflow_xcom_backend_default_bucket")
    PANDAS_DATAFRAME = "dataframe"

    @staticmethod
    def write_and_upload_value(value: Any) -> str:
        """Convert to string and upload to GCS"""
        key_str = GCSXComBackend.PREFIX + str(uuid.uuid4())
        hook = GCSHook(gcp_conn_id=GCSXComBackend.GCP_CONN_ID)
        if conf.getboolean("core", "enable_xcom_pickling"):
            value = pickle.dumps(value)
        elif isinstance(value, pd.DataFrame):
            value = value.to_json()
            key_str = key_str + "_" + GCSXComBackend.PANDAS_DATAFRAME
        else:
            value = json.dumps(value)
        hook.upload(GCSXComBackend.BUCKET_NAME, key_str, data=value)
        return key_str

    @staticmethod
    def read_value(filename: str) -> bytes:
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
        if isinstance(result, str) and result.startswith(GCSXComBackend.PREFIX):
            value = GCSXComBackend.read_value(result)
            if conf.getboolean("core", "enable_xcom_pickling"):
                return pickle.loads(value)  # nosec
            elif result.endswith(GCSXComBackend.PANDAS_DATAFRAME):
                return pd.read_json(value)
            return json.loads(value)

    def orm_deserialize_value(self) -> str:
        """
        Deserialize amethod which is used to reconstruct ORM XCom object.
        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom ORM model.
        """
        return f"XCOM is uploaded into GCS bucket: {GCSXComBackend.BUCKET_NAME}"
