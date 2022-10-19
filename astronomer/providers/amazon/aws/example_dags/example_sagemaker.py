from __future__ import annotations

import os
import uuid
from datetime import datetime

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerModelOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerTrainingOperatorAsync,
    SageMakerTransformOperatorAsync,
)

ENV_ID_KEY = os.getenv("ENV_ID_KEY", "")
ROLE_ARN_KEY = os.getenv("ROLE_ARN_KEY", "")
KNN_IMAGE_URI_KEY = os.getenv("KNN_IMAGE_URI_KEY", "")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")


DATASET = """
        5.1,3.5,1.4,0.2,Iris-setosa
        4.9,3.0,1.4,0.2,Iris-setosa
        7.0,3.2,4.7,1.4,Iris-versicolor
        6.4,3.2,4.5,1.5,Iris-versicolor
        4.9,2.5,4.5,1.7,Iris-virginica
        7.3,2.9,6.3,1.8,Iris-virginica
        """
SAMPLE_SIZE = DATASET.count("\n") - 1


@task
def set_up(env_id: str, knn_image_uri: str, role_arn: str) -> None:
    """Setting the details required for the ecr instance and sagemaker"""
    bucket_name = f"{env_id}-sagemaker-example"
    ecr_repository_name = f"{env_id}-repo"
    model_name = f"{env_id}-KNN-model"
    training_job_name = f"{env_id}-train-{uuid.uuid1()}"
    transform_job_name = f"{env_id}-transform"
    tuning_job_name = f"{env_id}-tune"

    input_data_S3_key = f"{env_id}/processed-input-data"
    prediction_output_s3_key = f"{env_id}/transform"
    raw_data_s3_key = f"{env_id}/preprocessing/input.csv"
    training_output_s3_key = f"{env_id}/results"

    resource_config = {
        "InstanceCount": 1,
        "InstanceType": "ml.m5.large",
        "VolumeSizeInGB": 1,
    }

    training_data_source = {
        "CompressionType": "None",
        "ContentType": "text/csv",
        "DataSource": {
            "S3DataSource": {
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3Uri": f"s3://{bucket_name}/{input_data_S3_key}/train.csv",
            }
        },
    }
    training_config = {
        "AlgorithmSpecification": {
            "TrainingImage": knn_image_uri,
            "TrainingInputMode": "File",
        },
        "HyperParameters": {
            "predictor_type": "classifier",
            "feature_dim": "4",
            "k": "3",
            "sample_size": str(SAMPLE_SIZE),
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
            "Image": knn_image_uri,
            "ModelDataUrl": f"s3://{bucket_name}/{training_output_s3_key}/{training_job_name}/output/model.tar.gz",
            # noqa: E501
        },
    }
    transform_config = {
        "TransformJobName": transform_job_name,
        "TransformInput": {
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{bucket_name}/{input_data_S3_key}/test.csv",
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
    }

    ti = get_current_context()["ti"]
    ti.xcom_push(key="bucket_name", value=bucket_name)
    ti.xcom_push(key="raw_data_s3_key", value=raw_data_s3_key)
    ti.xcom_push(key="ecr_repository_name", value=ecr_repository_name)
    ti.xcom_push(key="training_config", value=training_config)
    ti.xcom_push(key="training_job_name", value=training_job_name)
    ti.xcom_push(key="model_config", value=model_config)
    ti.xcom_push(key="model_name", value=model_name)
    ti.xcom_push(key="tuning_job_name", value=tuning_job_name)
    ti.xcom_push(key="transform_config", value=transform_config)
    ti.xcom_push(key="transform_job_name", value=transform_job_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_logs(env_id) -> None:
    """Delete the cloud watch log based on the log group name"""
    generated_logs = [
        # Format: ('log group name', 'log stream prefix')
        ("/aws/sagemaker/ProcessingJobs", env_id),
        ("/aws/sagemaker/TrainingJobs", env_id),
        ("/aws/sagemaker/TransformJobs", env_id),
    ]
    client = boto3.client("logs")

    for group, prefix in generated_logs:
        try:
            if prefix:
                log_streams = client.describe_log_streams(
                    logGroupName=group,
                    logStreamNamePrefix=prefix,
                )["logStreams"]

                for stream_name in [stream["logStreamName"] for stream in log_streams]:
                    client.delete_log_stream(logGroupName=group, logStreamName=stream_name)
        except ClientError as e:
            raise e


with DAG(
    dag_id="example_sagemaker_test",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:

    setup_aws_config = BashOperator(
        task_id="setup_aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    test_setup = set_up(
        env_id=ENV_ID_KEY,
        knn_image_uri=KNN_IMAGE_URI_KEY,
        role_arn=ROLE_ARN_KEY,
    )

    # [START howto_operator_sagemaker_training]
    train_model = SageMakerTrainingOperatorAsync(
        task_id="train_model",
        print_log=False,
        config=test_setup["training_config"],
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
    )
    # [END howto_operator_sagemaker_training]

    # # [START howto_operator_sagemaker_model]
    create_model = SageMakerModelOperator(
        task_id="create_model",
        config=test_setup["model_config"],
    )
    # [END howto_operator_sagemaker_model]

    # [START howto_operator_sagemaker_transform]
    test_model = SageMakerTransformOperatorAsync(
        task_id="test_model",
        config=test_setup["transform_config"],
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
    )
    # [END howto_operator_sagemaker_transform]

    # # [START howto_operator_sagemaker_delete_model]
    delete_model = SageMakerDeleteModelOperator(
        task_id="delete_model",
        config={"ModelName": test_setup["model_name"]},
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_sagemaker_delete_model]

    chain(
        setup_aws_config,
        test_setup,
        train_model,
        create_model,
        test_model,
        delete_model,
        delete_logs(ENV_ID_KEY),
    )
