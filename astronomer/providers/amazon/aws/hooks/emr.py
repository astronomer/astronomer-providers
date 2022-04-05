from typing import Any, Dict, Optional

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


class EmrJobFlowHookAsync(AwsBaseHookAsync):
    """
    EmrJobFlowHookAsync is wrapper Interact with AWS EMR.Using Aiobotocore client makes API
    call to get cluster-level details by job_flow_id.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHookAsync.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "emr"
        super().__init__(*args, **kwargs)

    async def get_cluster_details(self, job_flow_id: str) -> Dict[str, Any]:
        """
        Using Aiobotocore client makes API call to ``describe_cluster`` get the cluster
        details, from cluster details fetch the cluster status

        :param job_flow_id: job_flow_id to check the state of cluster
        """
        async with await self.get_client_async() as client:
            try:
                response: Dict[str, Any] = await client.describe_cluster(ClusterId=job_flow_id)
                return response
            except ClientError as error:
                raise error

    @staticmethod
    def state_from_response(response: Dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :return: current state of the cluster
        """
        cluster_state: str = response["Cluster"]["Status"]["State"]
        return cluster_state

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from EMR AWS API
        """
        cluster_status = response["Cluster"]["Status"]
        state_change_reason = cluster_status.get("StateChangeReason")
        print("state_change_reason ", state_change_reason)
        if state_change_reason:
            print("state_change_reason.get('Code', 'No code') ", state_change_reason.get("Code", "No code"))
            print(
                "state_change_reason.get('Message', 'No code') ",
                state_change_reason.get("Message", "unknown"),
            )
            return (
                f"for code: {state_change_reason.get('Code', 'No code')} "
                f"with message {state_change_reason.get('Message', 'Unknown')}"
            )
        return None
