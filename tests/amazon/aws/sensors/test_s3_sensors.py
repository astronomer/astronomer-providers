import unittest

import pytest
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3KeysUnchangedSensor

from astronomer.providers.amazon.aws.sensors.s3 import (
    S3KeySensorAsync,
    S3KeySizeSensorAsync,
    S3KeysUnchangedSensorAsync,
    S3PrefixSensorAsync,
)

MODULE = "astronomer.providers.amazon.aws.sensors.s3"


class TestS3KeySensorAsync:
    def test_init(self):
        task = S3KeySensorAsync(task_id="s3_key_sensor", bucket_key="file_in_bucket")
        assert isinstance(task, S3KeySensor)
        assert task.deferrable is True


class TestS3KeysUnchangedSensorAsync:
    def test_init(self):
        task = S3KeysUnchangedSensorAsync(
            task_id="s3_keys_unchanged_sensor", bucket_name="test_bucket", prefix="test"
        )
        assert isinstance(task, S3KeysUnchangedSensor)
        assert task.deferrable is True


class TestS3KeySizeSensorAsync(unittest.TestCase):
    def test_deprecation_warnings_generated(self):
        with pytest.warns(expected_warning=DeprecationWarning):
            S3KeySizeSensorAsync(task_id="s3_size_sensor", bucket_key="s3://test_bucket/file")


class TestS3PrefixSensorAsync(unittest.TestCase):
    def test_deprecation_warnings_generated(self):
        with pytest.warns(expected_warning=DeprecationWarning):
            S3PrefixSensorAsync(
                task_id="check_s3_prefix_sensor",
                bucket_name="test_bucket",
                prefix="test",
            )
