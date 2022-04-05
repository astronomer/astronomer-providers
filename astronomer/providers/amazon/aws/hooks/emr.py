from typing import Any

from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync


class EmrContainerHookAsync(AwsBaseHookAsync):
    """
    The EmrContainerHookAsync interact with AWS EMR EKS Virtual Cluster
    to run, poll jobs and return job status
    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    :param virtual_cluster_id: Cluster ID of the EMR on EKS virtual cluster
    :type virtual_cluster_id: str
    """

    def __init__(self, virtual_cluster_id: str, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "emr-containers"
        kwargs["resource_type"] = "emr-containers"
        super().__init__(*args, **kwargs)
        self.virtual_cluster_id = virtual_cluster_id

    async def check_job_status(self, job_id: str) -> Any:
        """
        Fetch the status of submitted job run. Returns None or one of valid query states.

        :param job_id: Id of submitted job run
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.describe_job_run(
                    virtualClusterId=self.virtual_cluster_id,
                    id=job_id,
                )
                if "jobRun" in response and "state" in response["jobRun"]:
                    return response["jobRun"]["state"]
                else:
                    return None
            except ClientError as e:
                raise e
