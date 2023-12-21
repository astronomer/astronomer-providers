import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.s3 import (
    S3KeyTrigger,
)


class TestS3KeyTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = S3KeyTrigger(
            bucket_key="s3://test_bucket/file", bucket_name="test_bucket", wildcard_match=True
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.amazon.aws.triggers.s3.S3KeyTrigger"
        assert kwargs == {
            "bucket_name": "test_bucket",
            "bucket_key": "s3://test_bucket/file",
            "wildcard_match": True,
            "use_regex": False,
            "aws_conn_id": "aws_default",
            "hook_params": {},
            "soft_fail": False,
            "poke_interval": 5.0,
            "should_check_fn": False,
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
    async def test_run_success(self, mock_client):
        """
        Test if the task is run is in triggerr successfully.
        """
        mock_client.return_value.check_key.return_value = True
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        with mock_client:
            task = asyncio.create_task(trigger.run().__anext__())
            await asyncio.sleep(0.5)

            assert task.done() is True
            asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.check_key")
    @mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
    async def test_run_pending(self, mock_client, mock_check_key):
        """
        Test if the task is run is in trigger successfully and set check_key to return false.
        """
        mock_check_key.return_value = False
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        with mock_client:
            task = asyncio.create_task(trigger.run().__anext__())
            await asyncio.sleep(0.5)

            assert task.done() is False
            asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
    async def test_run_exception(self, mock_client):
        """Test if the task is run is in case of exception."""
        mock_client.side_effect = Exception("Unable to locate credentials")
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {
                    "message": "Unable to locate credentials",
                    "status": "error",
                    "soft_fail": False,
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_client_async")
    @mock.patch("astronomer.providers.amazon.aws.triggers.s3.S3HookAsync.get_files")
    async def test_run_check_fn_success(self, mock_get_files, mock_client):
        """Test if the task is run is in trigger with check_fn."""

        mock_get_files.return_value = [{"Size": 123, "Key": "test.csv"}]
        mock_client.return_value.check_key.return_value = True
        trigger = S3KeyTrigger(
            bucket_key="s3://test_bucket/file",
            bucket_name="test_bucket",
            poke_interval=1,
            should_check_fn=True,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "running", "files": [{"Size": 123}]}) == actual
