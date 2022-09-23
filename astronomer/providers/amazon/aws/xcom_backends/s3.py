import json
import os
import uuid
from typing import Any, Union

import pandas as pd
from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3XComBackend(BaseXCom):
    """
    Custom XCom persistence class extends base to support various datatypes.
    To use this XCom Backend, add the environment variable `AIRFLOW__CORE__XCOM_BACKEND`
    to your environment and set it to
    `astronomer.providers.amazon.aws.xcom_backends.s3.S3XComBackend`
    """

    PREFIX = os.getenv("PREFIX", "s3_xcom_")
    AWS_CONN_ID = os.getenv("CONNECTION_NAME", "aws_default")
    BUCKET_NAME = os.getenv("XCOM_BACKEND_BUCKET_NAME", "airflow_xcom_backend_default_bucket")

    @staticmethod
    def write_and_upload_value(value: Any) -> str:
        """Convert to string and upload to S3"""
        key_str = S3XComBackend.PREFIX + str(uuid.uuid4())
        hook = S3Hook(aws_conn_id=S3XComBackend.AWS_CONN_ID)
        if isinstance(value, list):
            value = str(value)
        elif isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, pd.DataFrame):
            value = value.to_json()
        hook.load_string(bucket_name=S3XComBackend.BUCKET_NAME, key=key_str, string_data=value)
        return key_str

    @staticmethod
    def read_value(filepath: str) -> Union[str, bytes]:
        """Download the file from S3"""
        # Here we download the file from S3
        hook = S3Hook(aws_conn_id=S3XComBackend.AWS_CONN_ID)
        data = hook.download_file(bucket_name=S3XComBackend.BUCKET_NAME, key=filepath)
        return data

    @staticmethod
    def serialize_value(value: Any) -> Any:  # type: ignore[override]
        """Custom XCOM for S3 to serialize the data"""
        value = S3XComBackend.write_and_upload_value(value)
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        """Custom XCOM for S3 to deserialize the data"""
        result = BaseXCom.deserialize_value(result)
        # Check if result is string and has the configured XCom prefix
        if isinstance(result, str) and result.startswith(S3XComBackend.PREFIX):
            return S3XComBackend.read_value(result)
        return result

    def orm_deserialize_value(self) -> str:
        """
        Deserialize method which is used to reconstruct ORM XCom object.
        This method help to avoid unnecessary request or other
        resource-consuming operations when creating XCom ORM model.
        """
        return f"XCOM is uploaded into S3 bucket: {S3XComBackend.BUCKET_NAME}"
