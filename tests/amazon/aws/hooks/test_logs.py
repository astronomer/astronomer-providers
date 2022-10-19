import time
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.aws_logs import AwsLogsHookAsync


class TestAwsLogsHookAsync:
    TEST_CONN_ID = "test_conn_id"

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.get_client_async")
    async def test_describe_log_streams_async(self, mock_client):
        """Test AwsLogsHookAsync async hook function to describe_log_streams using Aiobotocore lib"""
        mock_client.return_value.__aenter__.return_value.describe_log_streams.return_value = {
            "test": "response"
        }
        hook = AwsLogsHookAsync(aws_conn_id=self.TEST_CONN_ID)
        result = await hook.describe_log_streams_async(
            "/aws/sagemaker/TrainingJobs", "test_job/", "LogStreamName", 1
        )
        assert result == {"test": "response"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.get_client_async")
    async def test_aws_log_resource_not_found_exception(self, mock_client):
        """Assert describe_log_streams method throw exception"""
        mock_client.return_value.__aenter__.return_value.describe_log_streams.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
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
            operation_name="logs",
        )
        hook = AwsLogsHookAsync(aws_conn_id=self.TEST_CONN_ID)
        response = await hook.describe_log_streams_async(
            "/aws/sagemaker/TrainingJobs", "test_job/", "LogStreamName", 1
        )
        assert response == {}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.get_client_async")
    async def test_aws_log_exception(self, mock_client):
        """Assert describe_log_streams method throw exception"""
        mock_client.return_value.__aenter__.return_value.describe_log_streams.side_effect = ClientError(
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
            operation_name="logs",
        )
        hook = AwsLogsHookAsync(aws_conn_id=self.TEST_CONN_ID)
        with pytest.raises(ClientError):
            await hook.describe_log_streams_async(
                "/aws/sagemaker/TrainingJobs", "test_job/", "LogStreamName", 1
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_skip",
        [1, 2],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.aws_logs.AwsLogsHookAsync.get_client_async")
    async def test_get_log_events(self, mock_client, mock_skip):
        log_group_name = "example-group"
        log_stream_name = "example-log-stream"
        time_stamp = int(time.time()) * 1000
        mock_client.return_value.__aenter__.return_value.get_log_events.return_value = {
            "events": [
                {"timestamp": time_stamp, "message": "Test Message 1", "ingestionTime": 123},
                {"timestamp": time_stamp, "message": "Test Message 2", "ingestionTime": 123},
            ],
            "nextForwardToken": "test",
            "nextBackwardToken": "test",
        }
        hook = AwsLogsHookAsync(aws_conn_id=self.TEST_CONN_ID)
        async_generator = hook.get_log_events_async(
            log_group=log_group_name, log_stream_name=log_stream_name, skip=mock_skip
        )
        await async_generator.asend(None)
        assert async_generator.__aiter__() is async_generator
