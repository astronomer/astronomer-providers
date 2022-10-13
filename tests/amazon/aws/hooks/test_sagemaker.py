from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
async def test_sagemaker_processing_job(mock_client):
    """Test sagemaker async hook function to describe the processing job using Aiobotocore lib"""
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_processing_job.return_value = {
        "test": "response"
    }
    hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
    result = await hook.describe_processing_job_async(job_name="test_job")
    assert result == {"test": "response"}


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
async def test_sagemaker_processing_job_exception(mock_client):
    """Assert describe_processing_job method throw exception"""
    # mocking async context function with return_value of __aenter__
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
async def test_sagemaker_transform_job(mock_client):
    """Test sagemaker async hook function to describe the transform job using Aiobotocore lib"""
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_transform_job.return_value = {
        "test": "response"
    }
    hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
    result = await hook.describe_transform_job_async(job_name="test_job")
    assert result == {"test": "response"}


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
async def test_sagemaker_transform_job_exception(mock_client):
    """Assert describe_transform_job method throw exception"""
    # mocking async context function with return_value of __aenter__
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
async def test_sagemaker_training_job(mock_client):
    """Test sagemaker async hook function to describe the training job using Aiobotocore lib"""
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_training_job.return_value = {"test": "response"}
    hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
    result = await hook.describe_training_job_async(job_name="test_job")
    assert result == {"test": "response"}


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.sagemaker.SageMakerHookAsync.get_client_async")
async def test_sagemaker_training_job_exception(mock_client):
    """Assert describe_training_job method throw exception"""
    # mocking async context function with return_value of __aenter__
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
