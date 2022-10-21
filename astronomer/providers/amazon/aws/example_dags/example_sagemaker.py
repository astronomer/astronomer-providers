from __future__ import annotations

import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperatorAsync,
    SageMakerTrainingOperatorAsync,
    SageMakerTransformOperatorAsync,
)

ROLE_ARN_KEY = os.getenv("SAGEMAKER_ROLE_ARN_KEY", "")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")
KNN_IMAGE_URI_KEY = os.getenv("KNN_IMAGE_URI_KEY", "")

DATASET = """
        9.0,0.38310254472482347,0.37403058828333824,0.3701814549305645,0.07801528813477883,0.0501548182716372,-0.09208298947092397,0.2957496481406288,0.0,1.0,0.0
        10.0,0.5080058356822007,0.676328456758846,0.489721303190709,0.4522080069316797,0.5434803957021823,0.4872212510777611,0.3316684995330287,0.0,1.0,0.0
        9.0,-0.3663172010194386,-0.3313311048261806,0.13110175841027571,-0.10551247585280192,-0.645907297828995,-0.4615604972224476,-0.4944650824921781,1.0,0.0,0.0
        10.0,-0.28304834038118715,-0.43209706098468337,-0.3470576346303013,-0.5510771028670953,-0.4837180668929254,-0.4980521029263018,-0.5555271298592587,0.0,1.0,0.0
        11.0,0.8827157085543321,0.7770944129173487,0.967880696231286,0.8457731008161586,0.712427511260588,1.1623159565990633,0.942288973203834,1.0,0.0,0.0
        """

TRANSFORM_DATASET = """
                    7.0,3.2,4.7,1.4
                    6.4,3.2,4.5,1.5
                    """
TRAIN_DATASET = """
                2,4.9,3.0,1.4,0.2
                0,7.3,2.9,6.3,1.8
                2,5.1,3.5,1.4,0.2
                0,4.9,2.5,4.5,1.7
                """


@task
def set_up(role_arn: str) -> None:
    """Setting the details required for the ecr instance and sagemaker"""
    bucket_name = f"amazon-sagemaker-example-{str(uuid.uuid4())}"
    ecr_repository_name = f"{ACCOUNT_ID}.dkr.ecr.{AWS_DEFAULT_REGION}.amazonaws.com/providers_repo"
    processing_job_name = f"{str(uuid.uuid4())}"
    processing_local_input_path = "/opt/ml/processing/input"
    processing_local_output_path = "/opt/ml/processing/output"
    raw_data_s3_key_input = "preprocessing/input/validation.csv"
    clean_data = "preprocessing/output/clean_data.csv"

    # Training configs
    training_job_name = f"provider-train-{str(uuid.uuid4())[:8]}"
    train_data_csv = "train-processed-input-data/train.csv"
    training_output_s3_key = "train-processed-output"

    model_name = "provider-KNN-model"

    # Transform configs
    transform_job_name = "provider-transform"
    transform_data_csv = "transform-processed-input-data/transform.csv"
    prediction_output_s3_key = "/transform"

    ecr_repository_image_uri = (
        f"{ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/providers_repo:sagemaker-processing"
    )
    resource_config = {
        "InstanceCount": 1,
        "InstanceType": "ml.m5.large",
        "VolumeSizeInGB": 1,
    }
    processing_config = {
        "ProcessingJobName": processing_job_name,
        "ProcessingInputs": [
            {
                "InputName": "input",
                "AppManaged": False,
                "S3Input": {
                    "S3Uri": f"s3://{bucket_name}/{raw_data_s3_key_input}",
                    "LocalPath": processing_local_input_path,
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File",
                    "S3DataDistributionType": "FullyReplicated",
                    "S3CompressionType": "None",
                },
            },
        ],
        "ProcessingOutputConfig": {
            "Outputs": [
                {
                    "OutputName": "output",
                    "S3Output": {
                        "S3Uri": f"s3://{bucket_name}/{clean_data}",
                        "LocalPath": processing_local_output_path,
                        "S3UploadMode": "EndOfJob",
                    },
                    "AppManaged": False,
                }
            ]
        },
        "ProcessingResources": {"ClusterConfig": resource_config},
        "StoppingCondition": {"MaxRuntimeInSeconds": 3000},
        "AppSpecification": {
            "ImageUri": ecr_repository_image_uri,
        },
        "RoleArn": role_arn,
    }

    training_data_source = {
        "CompressionType": "None",
        "ContentType": "text/csv",
        "DataSource": {
            "S3DataSource": {
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3Uri": f"s3://{bucket_name}/{train_data_csv}",
            }
        },
    }
    training_config = {
        "AlgorithmSpecification": {
            "TrainingImage": KNN_IMAGE_URI_KEY,
            "TrainingInputMode": "File",
        },
        "HyperParameters": {
            "predictor_type": "classifier",
            "feature_dim": "4",
            "k": "3",
            "sample_size": str(6),
        },
        "InputDataConfig": [
            {
                "ChannelName": "train",
                **training_data_source,  # type: ignore[arg-type]
            }
        ],
        "OutputDataConfig": {"S3OutputPath": f"s3://{bucket_name}/{training_output_s3_key}/"},
        "ResourceConfig": resource_config,
        "RoleArn": role_arn,
        "StoppingCondition": {"MaxRuntimeInSeconds": 6000},
        "TrainingJobName": training_job_name,
    }
    model_config = {
        "ExecutionRoleArn": role_arn,
        "ModelName": model_name,
        "PrimaryContainer": {
            "Mode": "SingleModel",
            "Image": KNN_IMAGE_URI_KEY,
            "ModelDataUrl": f"s3://{bucket_name}/{training_output_s3_key}/{training_job_name}/output/model.tar.gz",
            # noqa: E501
        },
    }

    transform_config = {
        "Transform": {
            "TransformJobName": transform_job_name,
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": f"s3://{bucket_name}/{transform_data_csv}",
                    }
                },
                "SplitType": "Line",
                "ContentType": "text/csv",
            },
            "TransformOutput": {"S3OutputPath": f"s3://{bucket_name}/{prediction_output_s3_key}"},
            "TransformResources": {
                "InstanceCount": 1,
                "InstanceType": "ml.m5.large",
            },
            "ModelName": model_name,
        },
        "Model": model_config,
    }

    ti = get_current_context()["ti"]
    ti.xcom_push(key="bucket_name", value=bucket_name)
    ti.xcom_push(key="raw_data_s3_key_input", value=raw_data_s3_key_input)
    ti.xcom_push(key="train_data_csv", value=train_data_csv)
    ti.xcom_push(key="transform_data_csv", value=transform_data_csv)
    ti.xcom_push(key="ecr_repository_name", value=ecr_repository_name)
    ti.xcom_push(key="processing_config", value=processing_config)
    ti.xcom_push(key="training_config", value=training_config)
    ti.xcom_push(key="training_job_name", value=training_job_name)
    ti.xcom_push(key="model_config", value=model_config)
    ti.xcom_push(key="model_name", value=model_name)
    ti.xcom_push(key="transform_config", value=transform_config)
    ti.xcom_push(key="transform_job_name", value=transform_job_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_logs() -> None:
    """Delete the cloud watch log based on the log group name"""
    generated_logs = [
        "/aws/sagemaker/ProcessingJobs",
        "/aws/sagemaker/TrainingJobs",
        "/aws/sagemaker/TransformJobs",
    ]
    import boto3

    client = boto3.client("logs")
    for group in generated_logs:
        if client.describe_log_streams(logGroupName=group)["logStreams"]:
            client.delete_log_group(logGroupName=group)


with DAG(
    dag_id="example_async_sagemaker",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    tags=["example", "sagemaker", "async", "AWS"],
) as dag:

    setup_aws_config = BashOperator(
        task_id="setup_aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    test_setup = set_up(
        role_arn=ROLE_ARN_KEY,
    )

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=test_setup["bucket_name"],
    )

    upload_dataset = S3CreateObjectOperator(
        task_id="upload_dataset",
        s3_bucket=test_setup["bucket_name"],
        s3_key=test_setup["raw_data_s3_key_input"],
        data=DATASET,
        replace=True,
    )

    upload_training_dataset = S3CreateObjectOperator(
        task_id="upload_training_dataset",
        s3_bucket=test_setup["bucket_name"],
        s3_key=test_setup["train_data_csv"],
        data=TRAIN_DATASET,
        replace=True,
    )

    upload_transform_dataset = S3CreateObjectOperator(
        task_id="upload_transform_dataset",
        s3_bucket=test_setup["bucket_name"],
        s3_key=test_setup["transform_data_csv"],
        data=TRANSFORM_DATASET,
        replace=True,
    )
    # [START howto_operator_sagemaker_processing_async]
    preprocess_raw_data = SageMakerProcessingOperatorAsync(
        task_id="preprocess_raw_data",
        config=test_setup["processing_config"],
    )
    # [END howto_operator_sagemaker_processing_async]

    # [START howto_operator_sagemaker_training_async]
    train_model = SageMakerTrainingOperatorAsync(
        task_id="train_model",
        print_log=False,
        config=test_setup["training_config"],
    )
    # [END howto_operator_sagemaker_training_async]

    # [START howto_operator_sagemaker_transform_async]
    test_model = SageMakerTransformOperatorAsync(
        task_id="test_model",
        config=test_setup["transform_config"],
    )
    # [END howto_operator_sagemaker_transform_async]

    delete_model = SageMakerDeleteModelOperator(
        task_id="delete_model",
        config={"ModelName": test_setup["model_name"]},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=test_setup["bucket_name"],
        force_delete=True,
    )

    chain(
        # TEST SETUP
        setup_aws_config,
        test_setup,
        create_bucket,
        upload_dataset,
        upload_training_dataset,
        upload_transform_dataset,
        # TEST BODY
        preprocess_raw_data,
        train_model,
        test_model,
        # TEST TEARDOWN
        delete_model,
        delete_bucket,
        delete_logs(),
    )
