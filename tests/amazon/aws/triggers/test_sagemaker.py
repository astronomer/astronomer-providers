import asyncio
import time
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync
from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerTrainingTrigger,
    SagemakerTransformTrigger,
)


class TestSagemakerTransformTrigger:
    TEST_JOB_NAME = "test_job_name"
    POLL_INTERVAL = 5
    END_TIME = time.time() + 60 * 60 * 24 * 7
    AWS_CONN_ID = "aws_test"

    def test_sagemaker_transform_trigger_serialization(self):
        """Asserts that the SagemakerTransformTrigger correctly serializes its arguments and classpath."""
        trigger = SagemakerTransformTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTransformTrigger"
        assert kwargs == {
            "job_name": self.TEST_JOB_NAME,
            "poll_interval": self.POLL_INTERVAL,
            "end_time": self.END_TIME,
            "aws_conn_id": self.AWS_CONN_ID,
        }

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_transform_job_async", return_value={"TransformJobStatus": "Success"}
    )
    async def test_sagemaker_transform_trigger_success(self, mock_transform_job):
        """Test the Trigger run method by mocking the response successfully."""
        trigger = SagemakerTransformTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "success", "message": "SageMaker Job completed"})

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync,
        "describe_transform_job_async",
        return_value={"TransformJobStatus": "Failed", "FailureReason": "Test Reason"},
    )
    async def test_sagemaker_transform_trigger_failed(self, mock_transform_job):
        """Test that SagemakerTransformTrigger fires the correct event in case of a failure."""
        trigger = SagemakerTransformTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent(
            {"status": "error", "message": "SageMaker job failed because Test Reason"}
        )

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_transform_job_async", return_value={"TransformJobStatus": "InProgress"}
    )
    async def test_sagemaker_transform_trigger_pending(self, mock_transform_job):
        """Test trigger run method iin pending state by mocking the response."""
        trigger = SagemakerTransformTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch.object(SageMakerHookAsync, "describe_transform_job_async")
    async def test_sagemaker_transform_trigger_timeout(self, mock_transform_job):
        """Test Trigger run method with if the task is timeout properly. by passing the end_time param"""
        trigger = SagemakerTransformTrigger(
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
        SageMakerHookAsync, "describe_transform_job_async", side_effect=Exception("test exception")
    )
    async def test_sagemaker_transform_trigger_exception(self, mock_transform_job):
        """
        Test sagemaker transform trigger with mocked exception as side effect
        of describe_transform_job_async function response.
        """
        trigger = SagemakerTransformTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "test exception"}) == actual


class TestSagemakerTrainingTrigger:
    TEST_JOB_NAME = "test_job_name"
    POLL_INTERVAL = 5
    END_TIME = time.time() + 60 * 60 * 24 * 7
    AWS_CONN_ID = "aws_test"

    def test_sagemaker_training_trigger_serialization(self):
        """Asserts that the SagemakerTrainingTrigger correctly serializes its arguments and classpath."""
        trigger = SagemakerTrainingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrainingTrigger"
        assert kwargs == {
            "job_name": self.TEST_JOB_NAME,
            "poll_interval": self.POLL_INTERVAL,
            "end_time": self.END_TIME,
            "aws_conn_id": self.AWS_CONN_ID,
        }

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_training_job_async", return_value={"TrainingJobStatus": "Success"}
    )
    async def test_sagemaker_training_trigger_success(self, mock_training_job):
        """Test the Trigger run method by mocking the response successfully."""
        trigger = SagemakerTrainingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "success", "message": "SageMaker Job completed"})

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync,
        "describe_training_job_async",
        return_value={"TrainingJobStatus": "Failed", "FailureReason": "Test Reason"},
    )
    async def test_sagemaker_training_trigger_failed(self, mock_training_job):
        """Test that SagemakerTrainingTrigger fires the correct event in case of a failure."""
        trigger = SagemakerTrainingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent(
            {"status": "error", "message": "SageMaker job failed because Test Reason"}
        )

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_training_job_async", return_value={"TrainingJobStatus": "InProgress"}
    )
    async def test_sagemaker_training_trigger_pending(self, mock_training_job):
        """Test trigger run method iin pending state by mocking the response."""
        trigger = SagemakerTrainingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch.object(SageMakerHookAsync, "describe_training_job_async")
    async def test_sagemaker_training_trigger_timeout(self, mock_training_job):
        """Test Trigger run method with if the task is timeout properly. by passing the end_time param"""
        trigger = SagemakerTrainingTrigger(
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
        SageMakerHookAsync, "describe_training_job_async", side_effect=Exception("test exception")
    )
    async def test_sagemaker_training_trigger_exception(self, mock_training_job):
        """
        Test sagemaker training trigger with mocked exception as side effect
        of describe_training_job_async function response.
        """
        trigger = SagemakerTrainingTrigger(
            job_name=self.TEST_JOB_NAME,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "test exception"}) == actual
