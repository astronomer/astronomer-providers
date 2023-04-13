import gzip
import json
import os
import pickle  # nosec
import uuid
from datetime import date, datetime
from io import BytesIO
from typing import Any

import pandas as pd
from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3XComBackend(BaseXCom):
    """
    The S3 custom xcom backend is an xcom custom backend wrapper that handles
    serialization and deserialization of common data types.
    This overrides the ``TaskInstance.XCom`` object with this wrapper.
    """

    @staticmethod
    def serialize_value(value: Any) -> Any:  # type: ignore[override]
        """Custom XCOM for S3 to serialize the data"""
        value = _S3XComBackend.write_and_upload_value(value)
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        """Custom XCOM for GCS to deserialize the data"""
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(_S3XComBackend.PREFIX):
            result = _S3XComBackend.download_and_read_value(result)
        return result

    def orm_deserialize_value(self) -> str:
        """
        Deserialize amethod which is used to reconstruct ORM XCom object.
        This method should be overridden in custom XCom backends to avoid
        unnecessary request or other resource consuming operations when
        creating XCom ORM model.
        """
        return f"XCOM is uploaded into S3 bucket: {_S3XComBackend.BUCKET_NAME}"


class _S3XComBackend:
    """
    Custom XCom persistence class extends base to support various datatypes.
    To use this XCom Backend, add the environment variable `AIRFLOW__CORE__XCOM_BACKEND`
    to your environment and set it to
    `astronomer.providers.amazon.aws.xcom_backends.s3.S3XComBackend`
    """

    PREFIX = os.getenv("XCOM_BACKEND_PREFIX", "s3_xcom_")
    AWS_CONN_ID = os.getenv("CONNECTION_NAME", "aws_default")
    BUCKET_NAME = os.getenv("XCOM_BACKEND_BUCKET_NAME", "airflow_xcom_backend_default_bucket")
    UPLOAD_CONTENT_AS_GZIP = os.getenv("XCOM_BACKEND_UPLOAD_CONTENT_AS_GZIP", False)
    PANDAS_DATAFRAME = "dataframe"
    DATETIME_OBJECT = "datetime"

    @staticmethod
    def write_and_upload_value(value: Any) -> str:
        """Convert to string and upload to S3"""
        key_str = f"{_S3XComBackend.PREFIX}{uuid.uuid4()}"
        hook = S3Hook(aws_conn_id=_S3XComBackend.AWS_CONN_ID)
        if conf.getboolean("core", "enable_xcom_pickling"):
            value = pickle.dumps(value)
        elif isinstance(value, pd.DataFrame):
            value = value.to_json()
            key_str = f"{key_str}_{_S3XComBackend.PANDAS_DATAFRAME}"
        elif isinstance(value, date):
            key_str = f"{key_str}_{_S3XComBackend.DATETIME_OBJECT}"
            value = value.isoformat()
        else:
            value = json.dumps(value)
        if _S3XComBackend.UPLOAD_CONTENT_AS_GZIP:
            key_str = f"{key_str}.gz"
            hook.load_string(
                bucket_name=_S3XComBackend.BUCKET_NAME, key=key_str, string_data=value, compression="gzip"
            )
        else:
            hook.load_string(bucket_name=_S3XComBackend.BUCKET_NAME, key=key_str, string_data=value)
        return key_str

    @staticmethod
    def download_and_read_value(filename: str) -> Any:
        """Download the file from S3"""
        # Here we download the file from S3
        hook = S3Hook(aws_conn_id=_S3XComBackend.AWS_CONN_ID)
        file = hook.download_file(
            bucket_name=_S3XComBackend.BUCKET_NAME, key=filename, preserve_file_name=True
        )
        with open(file, "rb") as f:
            data = f.read()
        if filename.endswith(".gz"):
            data = gzip.decompress(data)
            filename = filename.replace(".gz", "")
        if conf.getboolean("core", "enable_xcom_pickling"):
            return pickle.loads(data)  # nosec
        elif filename.endswith(_S3XComBackend.PANDAS_DATAFRAME):
            if isinstance(data, bytes):
                return pd.read_json(BytesIO(data))
            return pd.read_json(data)
        elif filename.endswith(_S3XComBackend.DATETIME_OBJECT):
            return datetime.fromisoformat(str(data))
        return json.loads(data)
