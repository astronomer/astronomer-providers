from typing import Dict, List, Union

from aiohttp import ClientError

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync
from astronomer.providers.amazon.aws.hooks.s3 import S3HookAsync


class SageMakerHookAsync(AwsBaseHookAsync):
    """
    Interact with Amazon SageMaker async using aiobotocore python library.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHookAsync.
    """

    def __init__(self, *args, **kwargs):
        kwargs["client_type"] = "sagemaker"
        super().__init__(client_type="sagemaker", *args, **kwargs)
        self.s3_hook = S3HookAsync(aws_conn_id=self.aws_conn_id)

    async def describe_transform_job_async(self, job_name: str) -> Dict[str, Union[str, List[str]]]:
        """
        Return the transform job info associated with the name

        :param job_name: the name of the transform job
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.describe_transform_job(TransformJobName=job_name)
                return response
            except ClientError as e:
                raise e

    async def describe_processing_job_async(self, job_name: str) -> Dict[str, Union[str, List[str]]]:
        """
        Return the processing job info associated with the name

        :param job_name: the name of the processing job
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.describe_processing_job(ProcessingJobName=job_name)
                return response
            except ClientError as e:
                raise e

    async def describe_training_job_async(self, job_name: str) -> Dict[str, Union[str, List[str]]]:
        """
        Return the training job info associated with the name

        :param job_name: the name of the training job
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.describe_training_job(TrainingJobName=job_name)
                return response
            except ClientError as e:
                raise e
