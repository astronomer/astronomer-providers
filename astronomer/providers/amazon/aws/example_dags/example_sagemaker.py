from __future__ import annotations

import base64
import os
import subprocess
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Any

import boto3
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
    SageMakerModelOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
    SageMakerTuningOperator,
)
from airflow.providers.amazon.aws.sensors.sagemaker import SageMakerTuningSensor
from airflow.utils.trigger_rule import TriggerRule

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperatorAsync,
)

ENV_ID_KEY = ""
# Externally fetched variables:
ROLE_ARN_KEY = ""
# The URI of a Docker image for handling KNN model training.
# To find the URI of a free Amazon-provided image that can be used, substitute your
# desired region in the following link and find the URI under "Registry Path".
# https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html#knn-us-east-1.title
# This URI should be in the format of {12-digits}.dkr.ecr.{region}.amazonaws.com/knn
KNN_IMAGE_URI_KEY = ""
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

# sys_test_context_task = (
#     SystemTestContextBuilder().add_variable(KNN_IMAGE_URI_KEY).add_variable(ROLE_ARN_KEY).build()
# )

# For this example we are using a subset of Fischer's Iris Data Set.
# The full dataset can be found at UC Irvine's machine learning repository:
# https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
DATASET = """
        5.1,3.5,1.4,0.2,Iris-setosa
        4.9,3.0,1.4,0.2,Iris-setosa
        7.0,3.2,4.7,1.4,Iris-versicolor
        6.4,3.2,4.5,1.5,Iris-versicolor
        4.9,2.5,4.5,1.7,Iris-virginica
        7.3,2.9,6.3,1.8,Iris-virginica
        """
SAMPLE_SIZE = DATASET.count("\n") - 1

# This script will be the entrypoint for the docker image which will handle preprocessing the raw data
# NOTE:  The following string must remain dedented as it is being written to a file.
# PREPROCESS_SCRIPT_TEMPLATE = """
# import boto3
# import numpy as np
# import pandas as pd
#
# def main():
#     # Load the Iris dataset from {input_path}/input.csv, split it into train/test
#     # subsets, and write them to {output_path}/ for the Processing Operator.
#
#     columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']
#     iris = pd.read_csv('{input_path}/input.csv', names=columns)
#
#     # Process data
#     iris['species'] = iris['species'].replace({{'Iris-virginica': 0, 'Iris-versicolor': 1, 'Iris-setosa': 2}})
#     iris = iris[['species', 'sepal_length', 'sepal_width', 'petal_length', 'petal_width']]
#
#     # Split into test and train data
#     iris_train, iris_test = np.split(
#         iris.sample(frac=1, random_state=np.random.RandomState()), [int(0.7 * len(iris))]
#     )
#
#     # Remove the "answers" from the test set
#     iris_test.drop(['species'], axis=1, inplace=True)
#
#     # Write the splits to disk
#     iris_train.to_csv('{output_path}/train.csv', index=False, header=False)
#     iris_test.to_csv('{output_path}/test.csv', index=False, header=False)
#
#     print('Preprocessing Done.')
#
# if __name__ == "__main__":
#     main()
#
# """


def _create_ecr_repository(repo_name: str) -> Any:
    return boto3.client("ecr").create_repository(repositoryName=repo_name)["repository"]["repositoryUri"]


def _build_and_upload_docker_image(preprocess_script: str, repository_uri: str) -> None:
    """
    We need a Docker image with the following requirements:
      - Has numpy, pandas, requests, and boto3 installed
      - Has our data preprocessing script mounted and set as the entry point
    """
    ecr_region = repository_uri.split(".")[3]

    # Fetch and parse ECR Token to be used for the docker push
    token = boto3.client("ecr", region_name=ecr_region).get_authorization_token()
    credentials = (base64.b64decode(token["authorizationData"][0]["authorizationToken"])).decode("utf-8")
    username, password = credentials.split(":")

    with NamedTemporaryFile(mode="w+t") as preprocessing_script, NamedTemporaryFile(mode="w+t") as dockerfile:
        preprocessing_script.write(preprocess_script)
        preprocessing_script.flush()

        dockerfile.write(
            f"""
            FROM amazonlinux
            COPY {preprocessing_script.name.split('/')[2]} /preprocessing.py
            ADD credentials /credentials
            ENV AWS_SHARED_CREDENTIALS_FILE=/credentials
            RUN yum install python3 pip -y
            RUN pip3 install boto3 pandas requests
            CMD [ "python3", "/preprocessing.py"]
            """
        )
        dockerfile.flush()

        docker_build_and_push_commands = f"""
            cp ~/.aws/credentials /tmp/credentials &&
            docker build -f {dockerfile.name} -t {repository_uri} /tmp &&
            rm /tmp/credentials &&
            aws ecr get-login-password --region {ecr_region} |
            docker login --username {username} --password {password} {repository_uri} &&
            docker push {repository_uri}
            """
        docker_build = subprocess.Popen(
            docker_build_and_push_commands,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, err = docker_build.communicate()

        if docker_build.returncode != 0:
            raise RuntimeError(err)


@task
def set_up(env_id: str, knn_image_uri: str, role_arn: str) -> None:
    """Setting the details required for the ecr instance and sagemaker"""
    bucket_name = f"{env_id}-sagemaker-example"
    ecr_repository_name = f"{env_id}-repo"
    model_name = f"{env_id}-KNN-model"
    processing_job_name = f"{env_id}-processing-10"
    training_job_name = f"{env_id}-train"
    transform_job_name = f"{env_id}-transform"
    tuning_job_name = f"{env_id}-tune"

    input_data_S3_key = f"{env_id}/processed-input-data"
    prediction_output_s3_key = f"{env_id}/transform"
    processing_local_input_path = "/opt/ml/processing/input"
    processing_local_output_path = "/opt/ml/processing/output"
    raw_data_s3_key = f"{env_id}/preprocessing/input.csv"
    training_output_s3_key = f"{env_id}/results"

    ecr_repository_uri = _create_ecr_repository(ecr_repository_name)
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
                    "S3Uri": f"s3://{bucket_name}/{raw_data_s3_key}",
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
                        "S3Uri": f"s3://{bucket_name}/{input_data_S3_key}",
                        "LocalPath": processing_local_output_path,
                        "S3UploadMode": "EndOfJob",
                    },
                    "AppManaged": False,
                }
            ]
        },
        "ProcessingResources": {
            "ClusterConfig": resource_config,
        },
        "StoppingCondition": {"MaxRuntimeInSeconds": 300},
        "AppSpecification": {
            "ImageUri": ecr_repository_uri,
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
            "ModelDataUrl": f"s3://{bucket_name}/{training_output_s3_key}/{training_job_name}/output/model.tar.gz",  # noqa: E501
        },
    }
    tuning_config = {
        "HyperParameterTuningJobName": tuning_job_name,
        "HyperParameterTuningJobConfig": {
            "Strategy": "Bayesian",
            "HyperParameterTuningJobObjective": {
                "MetricName": "test:accuracy",
                "Type": "Maximize",
            },
            "ResourceLimits": {
                # You would bump these up in production as appropriate.
                "MaxNumberOfTrainingJobs": 2,
                "MaxParallelTrainingJobs": 2,
            },
            "ParameterRanges": {
                "CategoricalParameterRanges": [],
                "IntegerParameterRanges": [
                    # Set the min and max values of the hyperparameters you want to tune.
                    {
                        "Name": "k",
                        "MinValue": "1",
                        "MaxValue": str(SAMPLE_SIZE),
                    },
                    {
                        "Name": "sample_size",
                        "MinValue": "1",
                        "MaxValue": str(SAMPLE_SIZE),
                    },
                ],
            },
        },
        "TrainingJobDefinition": {
            "StaticHyperParameters": {
                "predictor_type": "classifier",
                "feature_dim": "4",
            },
            "AlgorithmSpecification": {"TrainingImage": knn_image_uri, "TrainingInputMode": "File"},
            "InputDataConfig": [
                {
                    "ChannelName": "train",
                    **training_data_source,  # type: ignore[arg-type]
                },
                {
                    "ChannelName": "test",
                    **training_data_source,  # type: ignore[arg-type]
                },
            ],
            "OutputDataConfig": {"S3OutputPath": f"s3://{bucket_name}/{training_output_s3_key}"},
            "ResourceConfig": resource_config,
            "RoleArn": role_arn,
            "StoppingCondition": {"MaxRuntimeInSeconds": 60000},
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

    # preprocess_script = PREPROCESS_SCRIPT_TEMPLATE.format(
    #     input_path=processing_local_input_path, output_path=processing_local_output_path
    # )
    # _build_and_upload_docker_image(preprocess_script, ecr_repository_uri)

    ti = get_current_context()["ti"]
    ti.xcom_push(key="bucket_name", value=bucket_name)
    ti.xcom_push(key="raw_data_s3_key", value=raw_data_s3_key)
    ti.xcom_push(key="ecr_repository_name", value=ecr_repository_name)
    ti.xcom_push(key="processing_config", value=processing_config)
    ti.xcom_push(key="training_config", value=training_config)
    ti.xcom_push(key="training_job_name", value=training_job_name)
    ti.xcom_push(key="model_config", value=model_config)
    ti.xcom_push(key="model_name", value=model_name)
    ti.xcom_push(key="tuning_config", value=tuning_config)
    ti.xcom_push(key="tuning_job_name", value=tuning_job_name)
    ti.xcom_push(key="transform_config", value=transform_config)
    ti.xcom_push(key="transform_job_name", value=transform_job_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_ecr_repository(repository_name: str) -> None:
    """Delete the ECR instance with name which is created for running the sagemaker models"""
    client = boto3.client("ecr")

    # All images must be removed from the repo before it can be deleted.
    image_ids = client.list_images(repositoryName=repository_name)["imageIds"]
    client.batch_delete_image(
        repositoryName=repository_name,
        imageIds=[{"imageDigest": image["imageDigest"] for image in image_ids}],
    )
    client.delete_repository(repositoryName=repository_name)


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

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=test_setup["bucket_name"],
    )

    upload_dataset = S3CreateObjectOperator(
        task_id="upload_dataset",
        s3_bucket=test_setup["bucket_name"],
        s3_key=test_setup["raw_data_s3_key"],
        data=DATASET,
        replace=True,
    )

    # [START howto_operator_sagemaker_processing_async]
    preprocess_raw_data = SageMakerProcessingOperatorAsync(
        task_id="preprocess_raw_data",
        config=test_setup["processing_config"],
        action_if_job_exists="increment",
    )
    # [END howto_operator_sagemaker_processing_async]

    # [START howto_operator_sagemaker_training]
    train_model = SageMakerTrainingOperator(
        task_id="train_model",
        config=test_setup["training_config"],
    )
    # [END howto_operator_sagemaker_training]

    # [START howto_operator_sagemaker_model]
    create_model = SageMakerModelOperator(
        task_id="create_model",
        config=test_setup["model_config"],
    )
    # [END howto_operator_sagemaker_model]

    # [START howto_operator_sagemaker_tuning]
    tune_model = SageMakerTuningOperator(
        task_id="tune_model",
        config=test_setup["tuning_config"],
        # Waits by default, setting as False to demonstrate the Sensor below.
        wait_for_completion=False,
    )
    # [END howto_operator_sagemaker_tuning]

    # [START howto_sensor_sagemaker_tuning]
    await_tune = SageMakerTuningSensor(
        task_id="await_tuning",
        job_name=test_setup["tuning_job_name"],
    )
    # [END howto_sensor_sagemaker_tuning]

    # [START howto_operator_sagemaker_transform]
    test_model = SageMakerTransformOperator(
        task_id="test_model",
        config=test_setup["transform_config"],
        wait_for_completion=False,
    )
    # [END howto_operator_sagemaker_transform]

    # [START howto_operator_sagemaker_delete_model]
    delete_model = SageMakerDeleteModelOperator(
        task_id="delete_model",
        config={"ModelName": test_setup["model_name"]},
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_sagemaker_delete_model]

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
        # TEST BODY
        preprocess_raw_data,
        train_model,
        create_model,
        tune_model,
        await_tune,
        test_model,
        # TEST TEARDOWN
        delete_ecr_repository(test_setup["ecr_repository_name"]),
        delete_model,
        delete_bucket,
    )
