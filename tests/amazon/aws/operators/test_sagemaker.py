from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerTrainingOperatorAsync,
    SageMakerTransformOperatorAsync,
)
from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerTrainingWithLogTrigger,
    SagemakerTrigger,
)

CREATE_TRANSFORM_PARAMS: dict = {
    "TransformJobName": "job_name",
    "ModelName": "model_name",
    "MaxConcurrentTransforms": "12",
    "MaxPayloadInMB": "6",
    "BatchStrategy": "MultiRecord",
    "TransformInput": {"DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": "s3_uri"}}},
    "TransformOutput": {"S3OutputPath": "output_path"},
    "TransformResources": {"InstanceType": "ml.m4.xlarge", "InstanceCount": "3"},
}

CREATE_TRANSFORM_PARAMS: dict = {
    "TransformJobName": "test_transform_job_name",
    "TransformInput": {
        "DataSource": {
            "S3DataSource": {
                "S3DataType": "S3Prefix",
                "S3Uri": "s3://test/test_test/test.csv",
            }
        },
        "SplitType": "Line",
        "ContentType": "text/csv",
    },
    "TransformOutput": {"S3OutputPath": "s3://test/test_prediction"},
    "TransformResources": {
        "InstanceCount": 1,
        "InstanceType": "ml.m5.large",
    },
    "ModelName": "model_name",
}

CREATE_MODEL_PARAMS: dict = {
    "ModelName": "model_name",
    "PrimaryContainer": {"Image": "test_image", "ModelDataUrl": "output_path"},
    "ExecutionRoleArn": "arn:aws:iam:role/test-role",
}

CONFIG: dict = {"Model": CREATE_MODEL_PARAMS, "Transform": CREATE_TRANSFORM_PARAMS}

TRAINING_CONFIG = {
    "AlgorithmSpecification": {
        "TrainingImage": "test_knn",
        "TrainingInputMode": "File",
    },
    "HyperParameters": {
        "predictor_type": "classifier",
        "feature_dim": "4",
        "k": "3",
        "sample_size": "34",
    },
    "InputDataConfig": [
        {
            "ChannelName": "train",
        }
    ],
    "OutputDataConfig": {"S3OutputPath": "s3://test/test_prediction"},
    "ResourceConfig": "resource_config",
    "RoleArn": "arn:aws:iam:role/test-role",
    "StoppingCondition": {"MaxRuntimeInSeconds": 6000},
    "TrainingJobName": "training_job_name",
}


class TestSagemakerTransformOperatorAsync:
    TASK_ID = "test_sagemaker_transform_operator"
    CHECK_INTERVAL = 5
    MAX_INGESTION_TIME = 60 * 60 * 24 * 7

    @mock.patch.object(
        SageMakerHook,
        "create_transform_job",
        return_value={"TransformJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerHook, "list_transform_jobs", return_value=[])
    @mock.patch.object(SageMakerHook, "create_model", return_value=None)
    def test_sagemaker_transform_op_async(self, mock_hook, mock_transform_job, context):
        """Assert SageMakerTransformOperatorAsync deferred properly"""
        task = SageMakerTransformOperatorAsync(
            config=CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, SagemakerTrigger), "Trigger is not a SagemakerTrigger"

    @mock.patch.object(
        SageMakerHook,
        "create_transform_job",
        return_value={"TransformJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 404}},
    )
    @mock.patch.object(SageMakerHook, "list_transform_jobs", return_value=[])
    @mock.patch.object(SageMakerHook, "create_model", return_value=None)
    def test_sagemaker_transform_op_async_execute_failure(self, mock_hook, mock_transform_job, context):
        """Tests that an AirflowException is raised in case of error event from create_transform_job"""
        task = SageMakerTransformOperatorAsync(
            config=CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
        )
        with pytest.raises(AirflowException):
            task.execute(context)

    @pytest.mark.parametrize(
        "mock_event",
        [{"status": "error", "message": "test failure message"}],
    )
    def test_sagemaker_transform_op_async_execute_complete_failure(self, mock_event):
        """Tests that an AirflowException is raised in case of error event"""
        task = SageMakerTransformOperatorAsync(
            config=CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event=mock_event)

    @pytest.mark.parametrize(
        "mock_event",
        [{"status": "success", "message": "Job completed"}],
    )
    def test_sagemaker_transform_op_async_execute_complete(self, mock_event):
        """Asserts that logging occurs as expected"""
        task = SageMakerTransformOperatorAsync(
            config=CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("Job completed")


class TestSagemakerTrainingOperatorAsync:
    TASK_ID = "test_sagemaker_training_operator"
    CHECK_INTERVAL = 5
    MAX_INGESTION_TIME = 60 * 60 * 24 * 7

    @pytest.mark.parametrize(
        "mock_print_log_attr,mock_trigger_class, mock_trigger_name",
        [
            (True, SagemakerTrainingWithLogTrigger, "SagemakerTrainingWithLogTrigger"),
            (False, SagemakerTrigger, "SagemakerTrigger"),
        ],
    )
    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_training_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    @mock.patch.object(SageMakerHook, "list_training_jobs", return_value=[])
    def test_sagemaker_training_op_async(
        self,
        mock_training_job,
        mock_serialize,
        mock_create_training_job,
        mock_client,
        mock_print_log_attr,
        mock_trigger_class,
        mock_trigger_name,
    ):
        """Assert SageMakerTrainingOperatorAsync deferred properly"""
        mock_create_training_job.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        task = SageMakerTrainingOperatorAsync(
            config=TRAINING_CONFIG,
            task_id=self.TASK_ID,
            print_log=mock_print_log_attr,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(None)
        assert isinstance(exc.value.trigger, mock_trigger_class), f"Trigger is not a {mock_trigger_name}"

    @mock.patch.object(
        SageMakerHook,
        "create_training_job",
        return_value={"TrainingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 404}},
    )
    @mock.patch.object(SageMakerHook, "list_training_jobs", return_value=[])
    def test_sagemaker_training_op_async_execute_failure(self, mock_hook, mock_training_job):
        """Tests that an AirflowException is raised in case of error event from create_training_job"""
        task = SageMakerTrainingOperatorAsync(
            config=TRAINING_CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with pytest.raises(AirflowException):
            task.execute(None)

    @pytest.mark.parametrize(
        "mock_event",
        [{"status": "error", "message": "test failure message"}],
    )
    def test_sagemaker_training_op_async_execute_complete_failure(self, mock_event):
        """Tests that an AirflowException is raised in case of error event"""
        task = SageMakerTrainingOperatorAsync(
            config=TRAINING_CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event=mock_event)

    @pytest.mark.parametrize(
        "mock_event",
        [{"status": "success", "message": "Job completed"}],
    )
    def test_sagemaker_training_op_async_execute_complete(self, mock_event):
        """Asserts that logging occurs as expected"""
        task = SageMakerTrainingOperatorAsync(
            config=TRAINING_CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("Job completed")
