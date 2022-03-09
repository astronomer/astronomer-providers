import asyncio
import logging
from typing import Any, Dict

import botocore.exceptions

from astronomer.providers.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync

log = logging.getLogger(__name__)


class RedshiftHookAsync(AwsBaseHookAsync):
    """Interact with AWS Redshift using aiobotocore python library"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "redshift"
        kwargs["resource_type"] = "redshift"
        super().__init__(*args, **kwargs)

    async def cluster_status(self, cluster_identifier: str) -> Dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and get the status
        and returns the status of the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.describe_clusters(ClusterIdentifier=cluster_identifier)
                cluster_state = (
                    response["Clusters"][0]["ClusterStatus"] if response and response["Clusters"] else None
                )
                return {"status": "success", "cluster_state": cluster_state}
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error)}

    async def pause_cluster(self, cluster_identifier: str) -> Dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        pause the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        """
        try:
            async with await self.get_client_async() as client:
                response = await client.pause_cluster(ClusterIdentifier=cluster_identifier)
                status = response["Cluster"]["ClusterStatus"] if response and response["Cluster"] else None
                if status == "pausing":
                    flag = asyncio.Event()
                    while True:
                        expected_response = await asyncio.create_task(
                            self.get_cluster_status(cluster_identifier, "paused", flag)
                        )
                        await asyncio.sleep(10)
                        if flag.is_set():
                            return expected_response
                return {"status": "error", "cluster_state": status}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error)}

    async def resume_cluster(self, cluster_identifier: str) -> Dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        resume the cluster for the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.resume_cluster(ClusterIdentifier=cluster_identifier)
                status = response["Cluster"]["ClusterStatus"] if response and response["Cluster"] else None
                if status == "resuming":
                    flag = asyncio.Event()
                    while True:
                        expected_response = await asyncio.create_task(
                            self.get_cluster_status(cluster_identifier, "available", flag)
                        )
                        await asyncio.sleep(10)
                        if flag.is_set():
                            return expected_response
                return {"status": "error", "cluster_state": status}
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error)}

    async def get_cluster_status(
        self, cluster_identifier: str, expected_state: str, flag: asyncio.Event
    ) -> Dict[str, Any]:
        """
        Make call self.cluster_status to know the status and run till the expected_state is met and set the flag

        :param cluster_identifier: unique identifier of a cluster
        :param expected_state: expected_state example("available", "pausing", "paused"")
        :param flag: asyncio even flag set true if success and if any error
        """
        try:
            response = await self.cluster_status(cluster_identifier)
            if ("cluster_state" in response and response["cluster_state"] == expected_state) or response[
                "status"
            ] == "error":
                flag.set()
            return response
        except botocore.exceptions.ClientError as error:
            flag.set()
            return {"status": "error", "message": str(error)}
