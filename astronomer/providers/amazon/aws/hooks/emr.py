from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync


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

    async def get_job_failure_reason(self, job_id: str) -> Optional[str]:
        """
        Fetch the reason for a job failure (e.g. error message). Returns None or reason string.

        :param job_id: Id of submitted job run
        :return: str
        """
        # We absorb any errors if we can't retrieve the job status
        reason = None

        async with await self.get_client_async() as client:
            try:
                response = await client.describe_job_run(
                    virtualClusterId=self.virtual_cluster_id,
                    id=job_id,
                )
                failure_reason = response["jobRun"]["failureReason"]
                state_details = response["jobRun"]["stateDetails"]
                reason = f"{failure_reason} - {state_details}"
            except KeyError:
                self.log.exception("Could not get status of the EMR on EKS job")
            except ClientError as ex:
                self.log.exception("AWS request failed, check logs for more info: %s", ex.__str__())
        return reason


class EmrStepSensorHookAsync(AwsBaseHookAsync):
    """
    A thin wrapper to interact with AWS EMR API

    Additional arguments may be specified and are passed down to the underlying AwsBaseHook.

    For more details see here.
        - airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook

    :param job_flow_id: id of the JobFlow to add steps to
    :param step_id: step to check the state of
    :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
    :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
    :param config: Configuration for botocore client.
    """

    def __init__(self, job_flow_id: str, step_id: str, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "emr"
        kwargs["resource_type"] = "emr-containers"
        super().__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id

    async def emr_describe_step(self) -> Dict[str, Any]:
        """
        Make an API call with boto3 and get details about the cluster step.

        For AWS API definition see here::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

        :return: AWS EMR.Client.describe_step Api response Dict
        """
        async with await self.get_client_async() as client:
            try:
                response: Dict[str, Any] = await client.describe_step(
                    ClusterId=self.job_flow_id, StepId=self.step_id
                )
                self.log.debug("Response from AwsBaseHookAsync: %s", response)
                return response
            except ClientError as error:
                raise error

    @staticmethod
    def state_from_response(response: Dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :return: execution state of the cluster step
        """
        state: str = response["Step"]["Status"]["State"]
        return state

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        """
        fail_details = response["Step"]["Status"].get("FailureDetails")
        if fail_details:
            return (
                f"for reason {fail_details.get('Reason')} with message {fail_details.get('Message')} "
                f"and log file {fail_details.get('LogFile')}"
            )
        return None


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
