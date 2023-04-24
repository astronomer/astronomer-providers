from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperatorAsync,
    SageMakerTrainingOperatorAsync,
    SageMakerTransformOperatorAsync,
)
from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerProcessingTrigger,
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


class TestSagemakerProcessingOperatorAsync:
    TASK_ID = "test_sagemaker_processing_operator"
    CHECK_INTERVAL = 5
    MAX_INGESTION_TIME = 60 * 60 * 24 * 7

    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=False)
    def test_sagemakerprocessing_op_async(self, mock_hook, mock_processing, context):
        """Assert SageMakerProcessingOperatorAsync deferred properlu"""
        task = SageMakerProcessingOperatorAsync(
            config=CREATE_PROCESSING_PARAMS,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(
            exc.value.trigger, SagemakerProcessingTrigger
        ), "Trigger is not a SagemakerProcessingTrigger"

    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=True)
    def test_sagemakerprocessing_op_async_duplicate_failure(self, mock_hook, context):
        """Tests that an AirflowException is raised in case of error event from find_processing_job_name"""
        task = SageMakerProcessingOperatorAsync(
            config=CREATE_PROCESSING_PARAMS,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
        )
        with pytest.raises(AirflowException):
            task.execute(context)

    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 404}},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=False)
    def test_sagemakerprocessing_op_async_failure(self, mock_hook, mock_processing_job, context):
        """Tests that an AirflowException is raised in case of error event from create_processing_job"""
        task = SageMakerProcessingOperatorAsync(
            config=CREATE_PROCESSING_PARAMS,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
        )
        with pytest.raises(AirflowException):
            task.execute(context)

    @pytest.mark.parametrize(
        "event",
        [{"status": "error", "message": "test failure message"}, None],
    )
    def test_sagemakerprocessing_op_async_execute_failure(self, event):
        """Tests that an AirflowException is raised in case of error event"""
        task = SageMakerProcessingOperatorAsync(
            config=CREATE_PROCESSING_PARAMS,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event=event)

    @pytest.mark.parametrize(
        "event",
        [{"status": "success", "message": "Job completed"}],
    )
    def test_sagemakerprocessing_op_async_execute_complete(self, event):
        """Asserts that logging occurs as expected"""
        task = SageMakerProcessingOperatorAsync(
            config=CREATE_PROCESSING_PARAMS,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event=event)
        mock_log_info.assert_called_with("%s completed successfully.", "test_sagemaker_processing_operator")


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
            check_if_job_exists=False,
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
            check_if_job_exists=False,
            check_interval=self.CHECK_INTERVAL,
        )
        with pytest.raises(AirflowException):
            task.execute(context)

    @pytest.mark.parametrize(
        "mock_event",
        [{"status": "error", "message": "test failure message"}, None],
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
    @mock.patch.object(SageMakerHook, "describe_model")
    def test_sagemaker_transform_op_async_execute_complete(self, mock_model_output, mock_event):
        """Asserts that logging occurs as expected"""
        task = SageMakerTransformOperatorAsync(
            config=CONFIG,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
        )
        mock_model_output.return_value = {"test": "test"}
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("%s completed successfully.", "test_sagemaker_transform_operator")


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
            check_if_job_exists=False,
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
            check_if_job_exists=False,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        with pytest.raises(AirflowException):
            task.execute(None)

    @pytest.mark.parametrize(
        "mock_event",
        [{"status": "error", "message": "test failure message"}, None],
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
        mock_log_info.assert_called_with("%s completed successfully.", self.TASK_ID)
