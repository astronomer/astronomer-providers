import time
from datetime import datetime
from unittest import mock

import pytest
from airflow.providers.amazon.aws.hooks.sagemaker import LogState
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync

DESCRIBE_TRAINING_COMPLETED_RETURN = {
    "TrainingJobStatus": "Completed",
    "ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.c4.xlarge", "VolumeSizeInGB": 10},
    "TrainingStartTime": datetime(2018, 2, 17, 7, 15, 0, 103000),
    "TrainingEndTime": datetime(2018, 2, 17, 7, 19, 34, 953000),
    "ResponseMetadata": {
        "HTTPStatusCode": 200,
    },
}
DEFAULT_LOG_STREAMS = {"logStreams": [{"logStreamName": "test-job" + "/xxxxxxxxx"}]}
LIFECYCLE_LOG_STREAMS = [
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
    DEFAULT_LOG_STREAMS,
]

STREAM_LOG_EVENTS = [
    {"nextForwardToken": None, "events": [{"timestamp": 1, "message": "hi there #1"}]},
    {"nextForwardToken": None, "events": []},
    {
        "nextForwardToken": None,
        "events": [{"timestamp": 1, "message": "hi there #1"}, {"timestamp": 2, "message": "hi there #2"}],
    },
    {"nextForwardToken": None, "events": []},
    {
        "nextForwardToken": None,
        "events": [
            {"timestamp": 2, "message": "hi there #2"},
            {"timestamp": 2, "message": "hi there #2a"},
            {"timestamp": 3, "message": "hi there #3"},
        ],
    },
    {"nextForwardToken": None, "events": []},
]


class TestSagemakerHookAsync:
    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_sagemaker_processing_job(self, mock_client):
        """Test sagemaker async hook function to describe the processing job using Aiobotocore lib"""
        mock_client.return_value.__aenter__.return_value.describe_processing_job.return_value = {
            "test": "response"
        }
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        result = await hook.describe_processing_job_async(job_name="test_job")
        assert result == {"test": "response"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_sagemaker_processing_job_exception(self, mock_client):
        """Assert describe_processing_job method throw exception"""
        mock_client.return_value.__aenter__.return_value.describe_processing_job.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 404,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="sagemaker",
        )
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        with pytest.raises(ClientError):
            await hook.describe_processing_job_async(job_name="test_job")

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_sagemaker_transform_job(self, mock_client):
        """Test sagemaker async hook function to describe the transform job using Aiobotocore lib"""
        mock_client.return_value.__aenter__.return_value.describe_transform_job.return_value = {
            "test": "response"
        }
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        result = await hook.describe_transform_job_async(job_name="test_job")
        assert result == {"test": "response"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_sagemaker_transform_job_exception(self, mock_client):
        """Assert describe_transform_job method throw exception"""
        mock_client.return_value.__aenter__.return_value.describe_transform_job.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 404,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="sagemaker",
        )
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        with pytest.raises(ClientError):
            await hook.describe_transform_job_async(job_name="test_job")

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_sagemaker_training_job(self, mock_client):
        """Test sagemaker async hook function to describe the training job using Aiobotocore lib"""
        mock_client.return_value.__aenter__.return_value.describe_training_job.return_value = {
            "test": "response"
        }
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        result = await hook.describe_training_job_async(job_name="test_job")
        assert result == {"test": "response"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_sagemaker_training_job_exception(self, mock_client):
        """Assert describe_training_job method throw exception"""
        mock_client.return_value.__aenter__.return_value.describe_training_job.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 404,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="sagemaker",
        )
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        with pytest.raises(ClientError):
            await hook.describe_training_job_async(job_name="test_job")

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.sagemaker.argmin")
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.get_log_events_async")
    async def test_multi_stream_iter(self, mock_log_stream, mock_argmin):
        event = {"timestamp": int(time.time()) * 1000}
        mock_argmin.return_value = 1
        mock_log_stream.__aenter__.return_value = [event]
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        async_generator = hook.get_multi_stream("log", [None], {})
        await async_generator.asend(None)
        assert async_generator.__aiter__() is async_generator

    @pytest.mark.asyncio
    @mock.patch.object(time, "time")
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.get_client_async")
    @mock.patch(
        "astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.describe_training_job_async"
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.describe_log_streams_async")
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_describe_training_job_with_logs_in_progress(
        self, mock_client, mock_describe_log_stream, mock_desc_training_job, mock_log_client, mock_time
    ):
        mock_desc_training_job.return_value = DESCRIBE_TRAINING_COMPLETED_RETURN
        mock_describe_log_stream.side_effect = LIFECYCLE_LOG_STREAMS
        mock_log_client.return_value.__aenter__.return_value.get_log_events.side_effect = STREAM_LOG_EVENTS
        mock_time.return_value = 50
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        response = await hook.describe_training_job_with_log(
            job_name="test-job",
            positions={},
            stream_names=[],
            instance_count=1,
            state=LogState.WAIT_IN_PROGRESS,
            last_description={},
            last_describe_job_call=0,
        )
        assert response == (LogState.JOB_COMPLETE, {}, 50)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_job_log_state",
        [LogState.COMPLETE, LogState.JOB_COMPLETE],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.get_client_async")
    @mock.patch(
        "astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.describe_training_job_async"
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.describe_log_streams_async")
    @mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
    async def test_describe_training_job_with_logs_job_complete(
        self,
        mock_client,
        mock_describe_log_stream,
        mock_desc_training_job,
        mock_log_client,
        mock_job_log_state,
    ):
        mock_desc_training_job.return_value = DESCRIBE_TRAINING_COMPLETED_RETURN
        mock_describe_log_stream.side_effect = LIFECYCLE_LOG_STREAMS
        mock_log_client.return_value.__aenter__.return_value.get_log_events.side_effect = STREAM_LOG_EVENTS
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        response = await hook.describe_training_job_with_log(
            job_name="test-job",
            positions={},
            stream_names=[],
            instance_count=1,
            state=mock_job_log_state,
            last_description={},
            last_describe_job_call=0,
        )
        assert response == (LogState.COMPLETE, {}, 0)
