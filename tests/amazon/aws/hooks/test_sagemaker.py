from unittest import mock

import pytest

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync


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
    async def test_sagemaker_training_job(self, mock_client):
        """Test sagemaker async hook function to describe the training job using Aiobotocore lib"""
        mock_client.return_value.__aenter__.return_value.describe_training_job.return_value = {
            "test": "response"
        }
        hook = SageMakerHookAsync(aws_conn_id="test_conn_id")
        result = await hook.describe_training_job_async(job_name="test_job")
        assert result == {"test": "response"}
