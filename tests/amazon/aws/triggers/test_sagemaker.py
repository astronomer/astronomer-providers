import asyncio
import time
from datetime import datetime
from unittest import mock

import pytest
from airflow.providers.amazon.aws.hooks.sagemaker import LogState
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync
from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerProcessingTrigger,
    SagemakerTrainingWithLogTrigger,
    SagemakerTrigger,
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


class TestSagemakerTrigger:
    TEST_JOB_NAME = "test_job_name"
    POKE_INTERVAL = 5
    END_TIME = time.time() + 60 * 60 * 24 * 7
    AWS_CONN_ID = "aws_test"

    @pytest.mark.parametrize(
        "mock_job_type,mock_response_key",
        [
            ("Transform", "TransformJobStatus"),
            ("Training", "TrainingJobStatus"),
        ],
    )
    def test_sagemaker_trigger_serialization(self, mock_job_type, mock_response_key):
        """
        Asserts that the SagemakerTrigger correctly serializes its arguments and classpath.
        mock the job_type and response_key
        """
        trigger = SagemakerTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            job_type=mock_job_type,
            response_key=mock_response_key,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrigger"
        assert kwargs == {
            "job_name": self.TEST_JOB_NAME,
            "poke_interval": self.POKE_INTERVAL,
            "end_time": self.END_TIME,
            "aws_conn_id": self.AWS_CONN_ID,
            "job_type": mock_job_type,
            "response_key": mock_response_key,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_job_type,mock_response_key, mock_response, expected_response",
        [
            (
                "Transform",
                "TransformJobStatus",
                {"TransformJobStatus": "Success"},
                {"status": "success", "message": "SageMaker Job completed"},
            ),
            (
                "Transform",
                "TransformJobStatus",
                {"TransformJobStatus": "Failed", "FailureReason": "Test Reason"},
                {"status": "error", "message": "SageMaker job failed because Test Reason"},
            ),
            (
                "Training",
                "TrainingJobStatus",
                {"TrainingJobStatus": "Success"},
                {"status": "success", "message": "SageMaker Job completed"},
            ),
            (
                "Training",
                "TrainingJobStatus",
                {"TrainingJobStatus": "Failed", "FailureReason": "Test Reason"},
                {"status": "error", "message": "SageMaker job failed because Test Reason"},
            ),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrigger.get_job_status")
    async def test_sagemaker_trigger_success_and_failed(
        self, mock_job, mock_job_type, mock_response_key, mock_response, expected_response
    ):
        """Test the SagemakerTrigger run method by mocking the response with success state and failure state"""
        mock_job.return_value = mock_response
        trigger = SagemakerTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            job_type=mock_job_type,
            response_key=mock_response_key,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent(expected_response)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_job_type,mock_response_key, mock_response",
        [
            ("Transform", "TransformJobStatus", {"TransformJobStatus": "InProgress"}),
            ("Training", "TrainingJobStatus", {"TrainingJobStatus": "InProgress"}),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrigger.get_job_status")
    async def test_sagemaker_trigger_pending(self, mock_job, mock_job_type, mock_response_key, mock_response):
        """Test SagemakerTrigger run method in pending state by mocking the response and job_type, response_key."""
        mock_job.return_value = mock_response
        trigger = SagemakerTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            job_type=mock_job_type,
            response_key=mock_response_key,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_job_type,mock_response_key, mock_response",
        [
            ("Transform", "TransformJobStatus", {"TransformJobStatus": "InProgress"}),
            ("Training", "TrainingJobStatus", {"TrainingJobStatus": "InProgress"}),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrigger.get_job_status")
    async def test_sagemaker_transform_trigger_timeout(
        self, mock_job, mock_job_type, mock_response_key, mock_response
    ):
        """Test Trigger run method with if the task is timeout properly. by passing the end_time param"""
        mock_job.return_value = mock_response
        trigger = SagemakerTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=100,
            aws_conn_id=self.AWS_CONN_ID,
            job_type=mock_job_type,
            response_key=mock_response_key,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "Timeout"}) == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_job_type,mock_response_key",
        [
            ("Transform", "TransformJobStatus"),
            ("Training", "TrainingJobStatus"),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrigger.get_job_status")
    async def test_sagemaker_trigger_exception(self, mock_job, mock_job_type, mock_response_key):
        """
        Test SagemakerTrigger with mocked exception as side effect.
        """
        mock_job.side_effect = Exception("test exception")
        trigger = SagemakerTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            job_type=mock_job_type,
            response_key=mock_response_key,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "test exception"}) == actual

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_transform_job_async", return_value={"TrainingJobStatus": "Success"}
    )
    async def test_transform_get_job_status(self, mock_job):
        trigger = SagemakerTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            job_type="Transform",
            response_key="TransformJobStatus",
        )
        response = await trigger.get_job_status(SageMakerHookAsync, "TransformJobStatus", "Transform")
        assert response == {"TrainingJobStatus": "Success"}

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_training_job_async", return_value={"TrainingJobStatus": "Success"}
    )
    async def test_training_get_job_status(self, mock_job):
        trigger = SagemakerTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            job_type="Training",
            response_key="TrainingJobStatus",
        )
        response = await trigger.get_job_status(SageMakerHookAsync, "TrainingJobStatus", "Training")
        assert response == {"TrainingJobStatus": "Success"}


class TestSagemakerTrainingWithLogTrigger:
    TEST_JOB_NAME = "test_job_name"
    POKE_INTERVAL = 5
    END_TIME = time.time() + 60 * 60 * 24 * 7
    AWS_CONN_ID = "aws_test"
    STATUS = "InProgress"
    INSTANCE_COUNT = 1

    def test_sagemaker_training_trigger_serialization(self):
        """
        Asserts that the SagemakerTrainingWithLogTrigger correctly serializes its arguments and classpath.
        mock the job_type and response_key
        """
        trigger = SagemakerTrainingWithLogTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            status=self.STATUS,
            instance_count=self.INSTANCE_COUNT,
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath == "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrainingWithLogTrigger"
        )
        assert kwargs == {
            "job_name": self.TEST_JOB_NAME,
            "poke_interval": self.POKE_INTERVAL,
            "end_time": self.END_TIME,
            "aws_conn_id": self.AWS_CONN_ID,
            "status": self.STATUS,
            "instance_count": self.INSTANCE_COUNT,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_response, expected_response",
        [
            (
                (
                    LogState.COMPLETE,
                    {
                        "TrainingJobStatus": "Success",
                        "TrainingEndTime": datetime(2015, 1, 1),
                        "TrainingStartTime": datetime(2015, 1, 1),
                    },
                    time.time(),
                ),
                {"status": "success", "message": "SageMaker Job completed"},
            ),
            (
                (
                    LogState.COMPLETE,
                    {"TrainingJobStatus": "Failed", "FailureReason": "Test Reason"},
                    time.time(),
                ),
                {"status": "error", "message": "SageMaker job failed because Test Reason"},
            ),
        ],
    )
    @mock.patch.object(
        SageMakerHookAsync, "describe_training_job_async", return_value={"TrainingJobStatus": "InProgress"}
    )
    @mock.patch(
        "astronomer.providers.amazon.aws.triggers.sagemaker.SageMakerHookAsync.describe_training_job_with_log"
    )
    async def test_sagemaker_training_trigger_success_and_failed(
        self, mock_training_job, mock_job, mock_response, expected_response
    ):
        """
        Test the SagemakerTrainingWithLogTrigger run method by
        mocking the response with success state and failure state
        """
        mock_training_job.return_value = mock_response
        trigger = SagemakerTrainingWithLogTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            status=self.STATUS,
            instance_count=self.INSTANCE_COUNT,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent(expected_response)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_response",
        [(LogState.TAILING, {"TrainingJobStatus": "InProgress"}, time.time())],
    )
    @mock.patch.object(
        SageMakerHookAsync, "describe_training_job_async", return_value={"TrainingJobStatus": "InProgress"}
    )
    @mock.patch(
        "astronomer.providers.amazon.aws.triggers.sagemaker.SageMakerHookAsync.describe_training_job_with_log"
    )
    async def test_sagemaker_training_trigger_pending(
        self, mock_job, mock_describe_training_job, mock_response
    ):
        """Test SagemakerTrigger run method in pending state by mocking the response and job_type, response_key."""
        mock_job.return_value = mock_response
        trigger = SagemakerTrainingWithLogTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            status=self.STATUS,
            instance_count=self.INSTANCE_COUNT,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_response",
        [((LogState.TAILING, {"TrainingJobStatus": "InProgress"}, time.time()),)],
    )
    @mock.patch.object(
        SageMakerHookAsync, "describe_training_job_async", return_value={"TrainingJobStatus": "InProgress"}
    )
    @mock.patch(
        "astronomer.providers.amazon.aws.triggers.sagemaker.SageMakerHookAsync.describe_training_job_with_log"
    )
    async def test_sagemaker_training_trigger_timeout(
        self, mock_job, mock_describe_training_job, mock_response
    ):
        """Test Trigger run method with if the task is timeout properly. by passing the end_time param"""
        mock_job.return_value = mock_response
        trigger = SagemakerTrainingWithLogTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=100,
            aws_conn_id=self.AWS_CONN_ID,
            status=self.STATUS,
            instance_count=self.INSTANCE_COUNT,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent({"status": "error", "message": "SageMaker job took more than 100 seconds"}) == actual
        )

    @pytest.mark.asyncio
    @mock.patch.object(
        SageMakerHookAsync, "describe_training_job_async", return_value={"TrainingJobStatus": "InProgress"}
    )
    @mock.patch(
        "astronomer.providers.amazon.aws.triggers.sagemaker.SageMakerHookAsync.describe_training_job_with_log"
    )
    async def test_sagemaker_trigger_exception(self, mock_job, mock_describe_training_job):
        """
        Test SagemakerTrainingWithLogTrigger with mocked exception as side effect.
        """
        mock_job.side_effect = Exception("test exception")
        trigger = SagemakerTrainingWithLogTrigger(
            job_name=self.TEST_JOB_NAME,
            poke_interval=self.POKE_INTERVAL,
            end_time=self.END_TIME,
            aws_conn_id=self.AWS_CONN_ID,
            status=self.STATUS,
            instance_count=self.INSTANCE_COUNT,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": "test exception"}) == actual
