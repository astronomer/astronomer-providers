import gzip
import json
import os
import pickle  # nosec
import uuid
from datetime import date, datetime
from typing import Any

import pandas as pd
from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class GCSXComBackend(BaseXCom):
    """
    The GCS custom xcom backend is an xcom custom backend wrapper that handles serialization and deserialization
    of common data types. This overrides the ``TaskInstance.XCom`` object with this wrapper.
    """

    @staticmethod
    def serialize_value(value: Any) -> Any:  # type: ignore[override]
        """Custom XCOM for GCS to serialize the data"""
        value = _GCSXComBackend.write_and_upload_value(value)
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        """Custom XCOM for GCS to deserialize the data"""
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(_GCSXComBackend.PREFIX):
            result = _GCSXComBackend.download_and_read_value(result)
        return result

    def orm_deserialize_value(self) -> str:
        """
        Deserialize amethod which is used to reconstruct ORM XCom object.
        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom ORM model.
        """
        return f"XCOM is uploaded into GCS bucket: {_GCSXComBackend.BUCKET_NAME}"


class _GCSXComBackend:
    """
    Custom XCom persistence class extends base to support various datatypes.
    To use this XCom Backend, add the environment variable `AIRFLOW__CORE__XCOM_BACKEND`
    to your environment and set it to
    `astronomer.providers.google.cloud.xcom_backends.gcs.GCSXComBackend`
    """

    PREFIX = os.getenv("XCOM_BACKEND_PREFIX", "gcs_xcom_")
    GCP_CONN_ID = os.getenv("XCOM_BACKEND_CONNECTION_NAME", "google_cloud_default")
    BUCKET_NAME = os.getenv("XCOM_BACKEND_BUCKET_NAME", "airflow_xcom_backend_default_bucket")
    UPLOAD_CONTENT_AS_GZIP = os.getenv("XCOM_BACKEND_UPLOAD_CONTENT_AS_GZIP", False)
    PANDAS_DATAFRAME = "dataframe"
    DATETIME_OBJECT = "datetime"

    @staticmethod
    def write_and_upload_value(value: Any) -> str:
        """Convert to string and upload to GCS"""
        key_str = f"{_GCSXComBackend.PREFIX}{uuid.uuid4()}"
        hook = GCSHook(gcp_conn_id=_GCSXComBackend.GCP_CONN_ID)
        if conf.getboolean("core", "enable_xcom_pickling"):
            value = pickle.dumps(value)
        elif isinstance(value, pd.DataFrame):
            value = value.to_json()
            key_str = f"{key_str}_{_GCSXComBackend.PANDAS_DATAFRAME}"
        elif isinstance(value, date):
            key_str = f"{key_str}_{_GCSXComBackend.DATETIME_OBJECT}"
            value = value.isoformat()
        else:
            value = json.dumps(value)
        if _GCSXComBackend.UPLOAD_CONTENT_AS_GZIP:
            key_str = f"{key_str}.gz"
            hook.upload(bucket_name=_GCSXComBackend.BUCKET_NAME, object_name=key_str, data=value, gzip=True)
        else:
            hook.upload(bucket_name=_GCSXComBackend.BUCKET_NAME, object_name=key_str, data=value)
        return key_str

    @staticmethod
    def download_and_read_value(filename: str) -> Any:
        """Download the file from GCS"""
        # Here we download the file from GCS
        hook = GCSHook(gcp_conn_id=_GCSXComBackend.GCP_CONN_ID)
        data = hook.download(bucket_name=_GCSXComBackend.BUCKET_NAME, object_name=filename)
        if filename.endswith(".gz"):
            data = gzip.decompress(data)
            filename = filename.replace(".gz", "")
        if conf.getboolean("core", "enable_xcom_pickling"):
            return pickle.loads(data)  # nosec
        elif filename.endswith(_GCSXComBackend.PANDAS_DATAFRAME):
            return pd.read_json(data)
        elif filename.endswith(_GCSXComBackend.DATETIME_OBJECT):
            return datetime.fromisoformat(str(data))
        return json.loads(data)
