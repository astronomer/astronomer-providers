import unittest
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.variable import Variable
from airflow.utils import timezone
from parameterized import parameterized

from astronomer_operators.amazon.aws.sensors.s3 import S3KeySensorAsync
from astronomer_operators.amazon.aws.triggers.s3 import S3Trigger


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
            op.poke(None)

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
            op.poke(None)

    @parameterized.expand([["s3://bucket/key", None, "key", "bucket"], ["key", "bucket", "key", "bucket"]])
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_parse_bucket_key(self, key, bucket, parsed_key, parsed_bucket, mock_hook):
        mock_hook.return_value.check_for_key.return_value = False

        op = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        op.poke(None)

        assert op.bucket_key == parsed_key
        assert op.bucket_name == parsed_bucket

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    @mock.patch.object(S3KeySensorAsync, "poke")
    def test_s3_key_sensor_async(self, key, bucket, mock_poke, mock_hook):

        """
        Asserts that a task is deferred and an S3Trigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False
        mock_poke.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, S3Trigger), "Trigger is not a S3Trigger"

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    @mock.patch.object(S3KeySensorAsync, "poke")
    def test_s3_key_sensor_execute_complete(self, key, bucket, mock_poke, mock_hook):
        """
        Asserts that a task is deferred and an S3Trigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False
        mock_poke.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )
        assert sensor.execute_complete(context) is None

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    @mock.patch.object(S3KeySensorAsync, "poke")
    def test_s3_key_sensor_execute_failure(self, key, bucket, mock_poke, mock_hook):
        """
        Asserts that a task is deferred and an S3Trigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False
        mock_poke.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )
        with pytest.raises(AirflowException):
            sensor.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    @parameterized.expand(
        [
            ["s3://bucket/key", None],
            ["key", "bucket"],
        ]
    )
    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    @mock.patch.object(S3KeySensorAsync, "defer")
    @mock.patch.object(S3KeySensorAsync, "poke")
    @mock.patch("astronomer_operators.amazon.aws.sensors.s3.S3Trigger")
    def test_s3_key_sensor_async_with_mock_defer(
        self, key, bucket, mock_trigger, mock_poke, mock_defer, mock_hook
    ):
        """
        Asserts that a task is deferred and an S3Trigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False
        mock_poke.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async",
            bucket_key=key,
            bucket_name=bucket,
        )

        sensor.execute(context)

        mock_defer.assert_called()
        mock_defer.assert_called_once_with(
            timeout=None, trigger=mock_trigger.return_value, method_name="execute_complete"
        )

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_parse_bucket_key_from_jinja(self, mock_hook):
        mock_hook.return_value.check_for_key.return_value = False

        Variable.set("test_bucket_key", "s3://bucket/key")

        execution_date = timezone.datetime(2020, 1, 1)

        dag = DAG("test_s3_key", start_date=execution_date)
        op = S3KeySensorAsync(
            task_id="s3_key_sensor",
            bucket_key="{{ var.value.test_bucket_key }}",
            bucket_name=None,
            dag=dag,
        )

        dag_run = DagRun(dag_id=dag.dag_id, execution_date=execution_date, run_id="test")
        ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        context = ti.get_template_context()
        ti.render_templates(context)

        op.poke(None)

        assert op.bucket_key == "key"
        assert op.bucket_name == "bucket"

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_poke(self, mock_hook):
        op = S3KeySensorAsync(task_id="s3_key_sensor", bucket_key="s3://test_bucket/file")

        mock_check_for_key = mock_hook.return_value.check_for_key
        mock_check_for_key.return_value = False
        assert not op.poke(None)
        mock_check_for_key.assert_called_once_with(op.bucket_key, op.bucket_name)

        mock_hook.return_value.check_for_key.return_value = True
        assert op.poke(None)

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    def test_poke_wildcard(self, mock_hook):
        op = S3KeySensorAsync(
            task_id="s3_key_sensor", bucket_key="s3://test_bucket/file", wildcard_match=True
        )

        mock_check_for_wildcard_key = mock_hook.return_value.check_for_wildcard_key
        mock_check_for_wildcard_key.return_value = False
        assert not op.poke(None)
        mock_check_for_wildcard_key.assert_called_once_with(op.bucket_key, op.bucket_name)

        mock_check_for_wildcard_key.return_value = True
        assert op.poke(None)

    @mock.patch("airflow.providers.amazon.aws.sensors.s3.S3Hook")
    @mock.patch.object(S3KeySensorAsync, "poke")
    def test_s3_key_sensor_with_wildcard_async(self, mock_poke, mock_hook):
        """
        Asserts that a task with wildcard=True is deferred and an S3Trigger will be fired
        when the S3KeySensorAsync is executed.
        """
        mock_hook.check_for_key.return_value = False
        mock_poke.return_value = False

        sensor = S3KeySensorAsync(
            task_id="s3_key_sensor_async", bucket_key="s3://test_bucket/file", wildcard_match=True
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute(context)

        assert isinstance(exc.value.trigger, S3Trigger), "Trigger is not a S3Trigger"
