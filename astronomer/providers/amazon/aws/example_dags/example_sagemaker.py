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
from airflow.utils.trigger_rule import TriggerRule

from astronomer.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperatorAsync,
)

ROLE_ARN_KEY = os.getenv("SAGEMAKER_ROLE_ARN_KEY", "")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "")

DATASET = """
        9.0,0.38310254472482347,0.37403058828333824,0.3701814549305645,0.07801528813477883,0.0501548182716372,-0.09208298947092397,0.2957496481406288,0.0,1.0,0.0
        10.0,0.5080058356822007,0.676328456758846,0.489721303190709,0.4522080069316797,0.5434803957021823,0.4872212510777611,0.3316684995330287,0.0,1.0,0.0
        9.0,-0.3663172010194386,-0.3313311048261806,0.13110175841027571,-0.10551247585280192,-0.645907297828995,-0.4615604972224476,-0.4944650824921781,1.0,0.0,0.0
        10.0,-0.28304834038118715,-0.43209706098468337,-0.3470576346303013,-0.5510771028670953,-0.4837180668929254,-0.4980521029263018,-0.5555271298592587,0.0,1.0,0.0
        11.0,0.8827157085543321,0.7770944129173487,0.967880696231286,0.8457731008161586,0.712427511260588,1.1623159565990633,0.942288973203834,1.0,0.0,0.0
        """


@task
def set_up(role_arn: str) -> None:
    """Setting the details required for the ecr instance and sagemaker"""
    bucket_name = f"amazon-sagemaker-example-{str(uuid.uuid4())}"
    ecr_repository_name = f"{ACCOUNT_ID}.dkr.ecr.us-east-2.amazonaws.com/providers_repo"
    processing_job_name = f"{str(uuid.uuid4())}"
    processing_local_input_path = "/opt/ml/processing/input"
    processing_local_output_path = "/opt/ml/processing/output"
    raw_data_s3_key_input = "preprocessing/input/validation.csv"
    clean_data = "preprocessing/output/clean_data.csv"

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

    ti = get_current_context()["ti"]
    ti.xcom_push(key="bucket_name", value=bucket_name)
    ti.xcom_push(key="raw_data_s3_key_input", value=raw_data_s3_key_input)
    ti.xcom_push(key="ecr_repository_name", value=ecr_repository_name)
    ti.xcom_push(key="processing_config", value=processing_config)


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

    # [START howto_operator_sagemaker_processing_async]
    preprocess_raw_data = SageMakerProcessingOperatorAsync(
        task_id="preprocess_raw_data",
        config=test_setup["processing_config"],
    )
    # [END howto_operator_sagemaker_processing_async]

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
        # TEST TEARDOWN
        delete_bucket,
    )
