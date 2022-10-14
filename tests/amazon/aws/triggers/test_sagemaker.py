import asyncio
import time
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync
from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerProcessingTrigger,
)


class TestSagemakerProcessingTrigger:
    TEST_JOB_NAME = "test_job_name"
    POLL_INTERVAL = 5
    END_TIME = time.time() + 60 * 60 * 24 * 7
    AWS_CONN_ID = "aws_test"

    def test_sagemakerprocessing_trigger_serialization(self):
        """
        Asserts that the SagemakerProcessingTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = SagemakerProcessingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerProcessingTrigger"
        assert kwargs == {
            "job_name": self.TEST_JOB_NAME,
            "poll_interval": self.POLL_INTERVAL,
            "end_time": self.END_TIME,
            "aws_conn_id": self.AWS_CONN_ID,
        }

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_processing_job_async", return_value={"ProcessingJobStatus": "Success"}
    )
    async def test_sagemakerprocessing_trigger_success(self, mock_processing_job):
        """
        Test if the task is run is in triggerr successfully.
        """
        trigger = SagemakerProcessingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": {"ProcessingJobStatus": "Success"}}) == actual

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync,
        "describe_processing_job_async",
        return_value={"ProcessingJobStatus": "Failed", "FailureReason": "Failed"},
    )
    async def test_sagemakerprocessing_trigger_failed(self, mock_processing_job):
        """
        Test that SagemakerProcessingTrigger fires the correct event in case of a failure.
        """
        trigger = SagemakerProcessingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=None,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "SageMaker job failed because Failed"}) == actual

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync,
        "describe_processing_job_async",
        return_value={"ProcessingJobStatus": "InProgress"},
    )
    async def test_sagemakerprocessing_trigger_pending(self, mock_processing_job):
        """Test if the task is run is in trigger successfully."""
        trigger = SagemakerProcessingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=None,
            aws_conn_id=self.AWS_CONN_ID,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch.object(SageMakerHookAsync, "describe_processing_job_async")
    async def test_sagemakerprocessing_trigger_timeout(self, mock_processing_job):
        """Test if the task is timeout properly."""
        trigger = SagemakerProcessingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=100,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Timeout"}) == actual

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_processing_job_async", side_effect=Exception("test exception")
    )
    async def test_sagemakerprocessing_trigger_exception(self, mock_processing_job):
        """Test if the task is run is in case of exception."""
        trigger = SagemakerProcessingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=None,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "test exception"}) == actual
