import asyncio
from typing import Any, Dict, Optional

import botocore.exceptions

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync


class RedshiftHookAsync(AwsBaseHookAsync):
    """Interact with AWS Redshift using aiobotocore python library"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs["client_type"] = "redshift"
        kwargs["resource_type"] = "redshift"
        super().__init__(*args, **kwargs)

    async def cluster_status(self, cluster_identifier: str, delete_operation: bool = False) -> Dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and get the status
        and returns the status of the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :param delete_operation: whether the method has been called as part of delete cluster operation
        """
        async with await self.get_client_async() as client:
            try:
                response = await client.describe_clusters(ClusterIdentifier=cluster_identifier)
                cluster_state = (
                    response["Clusters"][0]["ClusterStatus"] if response and response["Clusters"] else None
                )
                return {"status": "success", "cluster_state": cluster_state}
            except botocore.exceptions.ClientError as error:
                if delete_operation and error.response.get("Error", {}).get("Code", "") == "ClusterNotFound":
                    return {"status": "success", "cluster_state": "cluster_not_found"}
                return {"status": "error", "message": str(error)}

    async def delete_cluster(
        self,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: Optional[str] = None,
        polling_period_seconds: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        deletes the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        :param polling_period_seconds: polling period in seconds to check for the status
        """
        try:
            final_cluster_snapshot_identifier = final_cluster_snapshot_identifier or ""

            async with await self.get_client_async() as client:
                response = await client.delete_cluster(
                    ClusterIdentifier=cluster_identifier,
                    SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
                    FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier,
                )
                status = response["Cluster"]["ClusterStatus"] if response and response["Cluster"] else None
                if status == "deleting":
                    flag = asyncio.Event()
                    while True:
                        expected_response = await asyncio.create_task(
                            self.get_cluster_status(cluster_identifier, "cluster_not_found", flag, True)
                        )
                        await asyncio.sleep(polling_period_seconds)
                        if flag.is_set():
                            return expected_response
                return {"status": "error", "cluster_state": status}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error)}

    async def pause_cluster(
        self, cluster_identifier: str, polling_period_seconds: float = 5.0
    ) -> Dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        pause the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :param polling_period_seconds: polling period in seconds to check for the status
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
                        await asyncio.sleep(polling_period_seconds)
                        if flag.is_set():
                            return expected_response
                return {"status": "error", "cluster_state": status}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error)}

    async def resume_cluster(
        self,
        cluster_identifier: str,
        polling_period_seconds: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        resume the cluster for the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :param polling_period_seconds: polling period in seconds to check for the status
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
                        await asyncio.sleep(polling_period_seconds)
                        if flag.is_set():
                            return expected_response
                return {"status": "error", "cluster_state": status}
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error)}

    async def get_cluster_status(
        self,
        cluster_identifier: str,
        expected_state: str,
        flag: asyncio.Event,
        delete_operation: bool = False,
    ) -> Dict[str, Any]:
        """
        Make call self.cluster_status to know the status and run till the expected_state is met and set the flag

        :param cluster_identifier: unique identifier of a cluster
        :param expected_state: expected_state example("available", "pausing", "paused"")
        :param flag: asyncio even flag set true if success and if any error
        :param delete_operation: whether the method has been called as part of delete cluster operation
        """
        try:
            response = await self.cluster_status(cluster_identifier, delete_operation=delete_operation)
            if ("cluster_state" in response and response["cluster_state"] == expected_state) or response[
                "status"
            ] == "error":
                flag.set()
            return response
        except botocore.exceptions.ClientError as error:
            flag.set()
            return {"status": "error", "message": str(error)}
