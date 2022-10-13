from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperatorAsync,
)
from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerProcessingTrigger,
)

CREATE_PROCESSING_PARAMS: dict = {
    "AppSpecification": {
        "ContainerArguments": ["container_arg"],
        "ContainerEntrypoint": ["container_entrypoint"],
        "ImageUri": "image_uri",
    },
    "Environment": {"key": "value"},
    "ExperimentConfig": {
        "ExperimentName": "experiment_name",
        "TrialComponentDisplayName": "trial_component_display_name",
        "TrialName": "trial_name",
    },
    "ProcessingInputs": [
        {
            "InputName": "analytics_input_name",
            "S3Input": {
                "LocalPath": "local_path",
                "S3CompressionType": "None",
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3Uri": "s3_uri",
            },
        }
    ],
    "ProcessingJobName": "job_name",
    "ProcessingOutputConfig": {
        "KmsKeyId": "kms_key_ID",
        "Outputs": [
            {
                "OutputName": "analytics_output_name",
                "S3Output": {
                    "LocalPath": "local_path",
                    "S3UploadMode": "EndOfJob",
                    "S3Uri": "s3_uri",
                },
            }
        ],
    },
    "ProcessingResources": {
        "ClusterConfig": {
            "InstanceCount": "2",
            "InstanceType": "ml.p2.xlarge",
            "VolumeSizeInGB": "30",
            "VolumeKmsKeyId": "kms_key",
        }
    },
    "RoleArn": "arn:aws:iam::0122345678910:role/SageMakerPowerUser",
    "Tags": [{"key": "value"}],
}


@pytest.fixture(scope="function")
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch.object(
    SageMakerHook,
    "create_processing_job",
    return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
)
@mock.patch.object(SageMakerHook, "find_processing_job_by_name", return_value=False)
def test_sagemakerprocessing_op_async(mock_hook, mock_processing):
    """Assert SageMakerProcessingOperatorAsync deferred properlu"""
    task = SageMakerProcessingOperatorAsync(
        config=CREATE_PROCESSING_PARAMS,
        task_id="test_sagemaker_operator",
        check_interval=5,
        max_ingestion_time=100,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(
        exc.value.trigger, SagemakerProcessingTrigger
    ), "Trigger is not a SagemakerProcessingTrigger"


@mock.patch.object(SageMakerHook, "find_processing_job_by_name", return_value=True)
def test_sagemakerprocessing_op_async_duplicate_failure(mock_hook):
    """Tests that an AirflowException is raised in case of error event from find_processing_job_name"""
    task = SageMakerProcessingOperatorAsync(
        config=CREATE_PROCESSING_PARAMS, task_id="test_sagemaker_operator", check_interval=5
    )
    with pytest.raises(AirflowException):
        task.execute(context)


@mock.patch.object(
    SageMakerHook,
    "create_processing_job",
    return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 404}},
)
@mock.patch.object(SageMakerHook, "find_processing_job_by_name", return_value=False)
def test_sagemakerprocessing_op_async_failure(mock_hook, mock_processing_job):
    """Tests that an AirflowException is raised in case of error event from create_processing_job"""
    task = SageMakerProcessingOperatorAsync(
        config=CREATE_PROCESSING_PARAMS, task_id="test_sagemaker_operator", check_interval=5
    )
    with pytest.raises(AirflowException):
        task.execute(context)


@pytest.mark.parametrize(
    "event",
    [{"status": "error", "message": "test failure message"}, None],
)
def test_sagemakerprocessing_op_async_execute_failure(event):
    """Tests that an AirflowException is raised in case of error event"""
    task = SageMakerProcessingOperatorAsync(
        config=CREATE_PROCESSING_PARAMS, task_id="test_sagemaker_operator", check_interval=5
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event=event)


@pytest.mark.parametrize(
    "event",
    [{"status": "success", "message": "Job completed"}],
)
def test_sagemakerprocessing_op_async_execute_complete(event):
    """Asserts that logging occurs as expected"""
    task = SageMakerProcessingOperatorAsync(
        config=CREATE_PROCESSING_PARAMS, task_id="test_sagemaker_operator", check_interval=5
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event=event)
    mock_log_info.assert_called_with("%s completed successfully.", "test_sagemaker_operator")
