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
        # mocking async context function with return_value of __aenter__
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
    async def test_aws_log_job_exception(self, mock_client):
        """Assert describe_log_streams method throw exception"""
        # mocking async context function with return_value of __aenter__
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
    async def test_get_log_events(self, mock_client):
        log_group_name = "example-group"
        log_stream_name = "example-log-stream"
        time_stamp = int(time.time()) * 1000
        mock_client.return_value.__aenter__.return_value.get_log_events.return_value = {
            "events": [{"timestamp": time_stamp, "message": "Test Message 1", "ingestionTime": 123}],
        }
        hook = AwsLogsHookAsync(aws_conn_id=self.TEST_CONN_ID)
        events = hook.get_log_events(log_group=log_group_name, log_stream_name=log_stream_name)
        assert events.__aiter__() is events
