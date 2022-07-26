import unittest
from datetime import timedelta
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.variable import Variable
from airflow.utils import timezone
from parameterized import parameterized

from astronomer.providers.amazon.aws.sensors.s3 import (
    S3KeySensorAsync,
    S3KeySizeSensorAsync,
    S3KeysUnchangedSensorAsync,
    S3PrefixSensorAsync,
)
from astronomer.providers.amazon.aws.triggers.s3 import (
    S3KeySizeTrigger,
    S3KeysUnchangedTrigger,
    S3KeyTrigger,
    S3PrefixTrigger,
)


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


class TestS3KeySensorAsync(unittest.TestCase):
    def test_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        op = S3KeySensorAsync(task_id="s3_key_sensor", bucket_key="file_in_bucket")
        with pytest.raises(AirflowException):
            op._resolve_bucket_and_key()

    def test_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        op = S3KeySensorAsync(
            task_id="s3_key_sensor", bucket_key="s3://test_bucket/file", bucket_name="test_bucket"
        )
        with pytest.raises(AirflowException):
            op._resolve_bucket_and_key()

    @parameterized.expand([["s3://bucket/key", None, "key", "bucket"], ["key", "bucket", "key", "bucket"]])
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.check_for_key")
    def test_parse_bucket_key(self, key, bucket, parsed_key, parsed_bucket, mock_check):

        mock_check.return_value = False

        op = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        op._resolve_bucket_and_key()

        assert op.bucket_key == parsed_key
        assert op.bucket_name == parsed_bucket

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_sensor_async(self, key, bucket, mock_hook):
        """
        Asserts that a task is deferred and an S3KeyTrigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, S3KeyTrigger), "Trigger is not a S3KeyTrigger"

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_sensor_execute_complete_success(self, key, bucket, mock_hook):
        """
        Asserts that a task is completed with success status.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )
        assert sensor.execute_complete(context={}, event={"status": "success"}) is None

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_sensor_execute_complete_error(self, key, bucket, mock_hook):
        """
        Asserts that a task is completed with error status.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )
        with pytest.raises(AirflowException):
            sensor.execute_complete(context={}, event={"status": "error", "message": "mocked error"})

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    @mock.patch.object(S3KeySensorAsync, "defer")
    @mock.patch("astronomer.providers.amazon.aws.sensors.s3.S3KeyTrigger")
    def test_s3_key_sensor_async_with_mock_defer(self, key, bucket, mock_trigger, mock_defer, mock_hook):
        """
        Asserts that a task is deferred and an S3KeyTrigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        sensor.execute(context)

        mock_defer.assert_called()
        mock_defer.assert_called_once_with(
            timeout=timedelta(days=7), trigger=mock_trigger.return_value, method_name="execute_complete"
        )

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.check_for_key")
    def test_parse_bucket_key_from_jinja(self, mock_check):
        mock_check.return_value = False

        Variable.set("test_bucket_key", "s3://bucket/key")

        execution_date = timezone.datetime(2020, 1, 1)

        dag = DAG("test_s3_key", start_date=execution_date)
        op = S3KeySensorAsync(
            task_id="s3_key_sensor",
            bucket_key="s3://bucket/key",
            bucket_name=None,
            dag=dag,
        )

        dag_run = DagRun(dag_id=dag.dag_id, execution_date=execution_date, run_id="test")
        ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        context = ti.get_template_context()
        ti.render_templates(context)
        op._resolve_bucket_and_key()

        assert op.bucket_key == "key"
        assert op.bucket_name == "bucket"

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_sensor_with_wildcard_async(self, mock_hook):
        """
        Asserts that a task with wildcard=True is deferred and an S3KeyTrigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async", bucket_key="s3://test_bucket/file", wildcard_match=True
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, S3KeyTrigger), "Trigger is not a S3KeyTrigger"


class TestS3KeySizeSensorAsync(unittest.TestCase):
    def test_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        op = S3KeySizeSensorAsync(task_id="s3_key_size_sensor", bucket_key="file_in_bucket")
        with pytest.raises(AirflowException):
            op._resolve_bucket_and_key()

    def test_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        op = S3KeySizeSensorAsync(
            task_id="s3_key_size_sensor", bucket_key="s3://test_bucket/file", bucket_name="test_bucket"
        )
        with pytest.raises(AirflowException):
            op._resolve_bucket_and_key()

    @parameterized.expand([["s3://bucket/key", None, "key", "bucket"], ["key", "bucket", "key", "bucket"]])
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook.check_for_key")
    def test_parse_bucket_key(self, key, bucket, parsed_key, parsed_bucket, mock_check):
        """
        Assert if the S3KeySizeSensorAsync parsed the bucket_name and bucket_key
        correctly.
        """
        mock_check.return_value = False

        op = S3KeySizeSensorAsync(
            task_id="s3_key_size_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        op._resolve_bucket_and_key()

        assert op.bucket_key == parsed_key
        assert op.bucket_name == parsed_bucket

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_sensor_async(self, key, bucket, mock_hook):
        """
        Asserts that a task is deferred and an S3KeySizeTrigger will be fired
        when the S3KeySizeSensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySizeSensorAsync(
            task_id="s3_key_size_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, S3KeySizeTrigger), "Trigger is not a S3KeySizeTrigger"

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_size_sensor_execute_complete_success(self, key, bucket, mock_hook):
        """
        Asserts that a task is completed with success status
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySizeSensorAsync(
            task_id="s3_key_size_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )
        assert sensor.execute_complete(context={}, event={"status": "success"}) is None

    @parameterized.expand(
        [
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_size_sensor_execute_complete_error(self, key, bucket, mock_hook):
        """
        Asserts that a task is completed with error.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySizeSensorAsync(
            task_id="s3_key_size_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )
        with pytest.raises(AirflowException):
            sensor.execute_complete(context={}, event={"status": "error", "message": "Mocked error"})

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    @mock.patch.object(S3KeySizeSensorAsync, "defer")
    @mock.patch("astronomer.providers.amazon.aws.sensors.s3.S3KeySizeTrigger")
    def test_s3_key_sensor_async_with_mock_defer(self, key, bucket, mock_trigger, mock_defer, mock_hook):
        """
        Asserts that a task is deferred and an S3KeySizeSensorAsync will be fired
        when the S3KeySizeSensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySizeSensorAsync(
            task_id="s3_key_size_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        sensor.execute(context)

        mock_defer.assert_called()
        mock_defer.assert_called_once_with(
            timeout=timedelta(days=7), trigger=mock_trigger.return_value, method_name="execute_complete"
        )

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_key_size_sensor_with_wildcard_async(self, mock_hook):
        """
        Asserts that a task with wildcard=True is deferred and an S3KeyTrigger will be fired
        when the S3KeySizeSensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeySizeSensorAsync(
            task_id="s3_key_size_sensor_async", bucket_key="s3://test_bucket/file", wildcard_match=True
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, S3KeySizeTrigger), "Trigger is not a S3KeySizeTrigger"


class TestS3KeysUnchangedSensorAsync(unittest.TestCase):
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_keys_unchanged_sensor_check_trigger_instance(self, mock_hook):
        """
        Asserts that a task is deferred and an S3KeysUnchangedTrigger will be fired
        when the S3KeysUnchangedSensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeysUnchangedSensorAsync(
            task_id="s3_keys_unchanged_sensor", bucket_name="test_bucket", prefix="test"
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(
            exc.value.trigger, S3KeysUnchangedTrigger
        ), "Trigger is not a S3KeysUnchangedTrigger"

    @parameterized.expand([["bucket", "test"]])
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_keys_unchanged_sensor_execute_complete_success(self, bucket, prefix, mock_hook):
        """
        Asserts that a task completed with success status
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeysUnchangedSensorAsync(
            task_id="s3_keys_unchanged_sensor",
            bucket_name=bucket,
            prefix=prefix,
        )
        assert sensor.execute_complete(context={}, event={"status": "success"}) is None

    @parameterized.expand([["bucket", "test"]])
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_s3_keys_unchanged_sensor_execute_complete_error(self, bucket, prefix, mock_hook):
        """
        Asserts that a task is completed with error.
        """
        mock_hook.check_for_key.return_value = False

        sensor = S3KeysUnchangedSensorAsync(
            task_id="s3_keys_unchanged_sensor",
            bucket_name=bucket,
            prefix=prefix,
        )
        with pytest.raises(AirflowException):
            sensor.execute_complete(context={}, event={"status": "error", "message": "Mocked error"})

    def test_s3_keys_unchanged_sensor_raise_value_error(self):
        """
        Test if the S3KeysUnchangedTrigger raises Value error for negative inactivity_period.
        """
        with pytest.raises(ValueError):
            S3KeysUnchangedSensorAsync(
                task_id="s3_keys_unchanged_sensor",
                bucket_name="test_bucket",
                prefix="test",
                inactivity_period=-100,
            )


class TestS3PrefixSensorAsync(unittest.TestCase):
    def test_s3_prefix_sensor_async(self):
        """
        Asserts that a task is deferred and an S3PrefixTrigger will be fired
        when the S3PrefixSensorAsync is executed.
        """
        sensor = S3PrefixSensorAsync(
            task_id="s3_prefix_sensor_async", bucket_name="test-bucket", prefix=["prefix1", "prefix2"]
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, S3PrefixTrigger), "Trigger is not a S3PrefixSensorTrigger"

    def test_s3_prefix_sensor_execute_complete(self):
        """
        Asserts that a task is deferred and an S3PrefixTrigger will be fired
        when the S3PrefixSensorAsync is executed.
        """
        sensor = S3PrefixSensorAsync(
            task_id="s3_prefix_sensor_async", bucket_name="test-bucket", prefix=["prefix1", "prefix2"]
        )

        with mock.patch.object(sensor.log, "info") as mock_log_info:
            sensor.execute_complete(
                context, event={"status": "success", "message": "Success criteria met. Exiting."}
            )
        mock_log_info.assert_called_with("Success criteria met. Exiting.")

    def test_s3_prefix_sensor_execute_failure(self):
        """Tests that an AirflowException is raised in case of error event"""

        sensor = S3PrefixSensorAsync(
            task_id="s3_prefix_sensor_async", bucket_name="test-bucket", prefix=["prefix1", "prefix2"]
        )

        with pytest.raises(AirflowException):
            sensor.execute_complete(context, event={"status": "error", "message": "test failure message"})
