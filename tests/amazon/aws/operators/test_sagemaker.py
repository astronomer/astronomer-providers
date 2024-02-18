from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperatorAsync,
    SageMakerTrainingOperatorAsync,
    SageMakerTransformOperatorAsync,
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

    def test_init(self):
        task = SageMakerProcessingOperatorAsync(
            config=CREATE_PROCESSING_PARAMS,
            task_id=self.TASK_ID,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        assert isinstance(task, SageMakerProcessingOperator)
        assert task.deferrable is True


class TestSagemakerTransformOperatorAsync:
    TASK_ID = "test_sagemaker_transform_operator"
    CHECK_INTERVAL = 5
    MAX_INGESTION_TIME = 60 * 60 * 24 * 7

    def test_init(self):
        task = SageMakerTransformOperatorAsync(
            config=CONFIG,
            task_id=self.TASK_ID,
            check_if_job_exists=False,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        assert isinstance(task, SageMakerTransformOperator)
        assert task.deferrable is True


class TestSagemakerTrainingOperatorAsync:
    TASK_ID = "test_sagemaker_training_operator"
    CHECK_INTERVAL = 5
    MAX_INGESTION_TIME = 60 * 60 * 24 * 7

    def test_init(self):
        task = SageMakerTrainingOperatorAsync(
            config=TRAINING_CONFIG,
            task_id=self.TASK_ID,
            check_if_job_exists=False,
            check_interval=self.CHECK_INTERVAL,
            max_ingestion_time=self.MAX_INGESTION_TIME,
        )
        assert isinstance(task, SageMakerTrainingOperator)
        assert task.deferrable is True
