import logging
from typing import List, Optional

from astronomer_operators.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync

log = logging.getLogger(__name__)


class RedshiftHookAsync(AwsBaseHookAsync):
    """
    Interact with AWS Redshift using aiobotocore python library
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "redshift"
        kwargs["resource_type"] = "redshift"
        super().__init__(*args, **kwargs)

    async def cluster_status(self, cluster_identifier: str) -> str:
        """
        Connects to the AWS redshift cluster via aiobotocore and get the status
        and returns the status of the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        async with await self.get_client_async() as client:
            try:
                response = client.describe_clusters(ClusterIdentifier=cluster_identifier)
                response = await response
                return response["Clusters"][0]["ClusterStatus"] if response and response["Clusters"] else None
            except Exception as error:
                print(error)
                return "cluster_not_found"

    async def delete_cluster(
        self,
        cluster_identifier: str,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: Optional[str] = None,
    ):
        """
        Delete a cluster and optionally create a snapshot
        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        :param skip_final_cluster_snapshot: determines cluster snapshot creation
        :type skip_final_cluster_snapshot: bool
        :param final_cluster_snapshot_identifier: name of final cluster snapshot
        :type final_cluster_snapshot_identifier: str
        """
        final_cluster_snapshot_identifier = final_cluster_snapshot_identifier or ""
        try:
            async with await self.get_client_async() as client:
                response = client.delete_cluster(
                    ClusterIdentifier=cluster_identifier,
                    SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
                    FinalClusterSnapshotIdentifier=final_cluster_snapshot_identifier,
                )
                response = await response
                return response["Cluster"] if response["Cluster"] else None
        except Exception as error:
            print(error)
            raise error

    async def describe_cluster_snapshots(self, cluster_identifier: str) -> Optional[List[str]]:
        """
        Gets a list of snapshots for a cluster
        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        try:
            async with await self.get_client_async() as client:
                response = client.describe_cluster_snapshots(ClusterIdentifier=cluster_identifier)
                response = await response
                if "Snapshots" not in response:
                    print(None)
                snapshots = response["Snapshots"]
                snapshots = [snapshot for snapshot in snapshots if snapshot["Status"]]
                snapshots.sort(key=lambda x: x["SnapshotCreateTime"], reverse=True)
                print(snapshots)
                return snapshots
        except Exception as error:
            print(error)
            raise error

    async def pause_cluster(self, cluster_identifier: str) -> str:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        pause the cluster based on the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        try:
            async with await self.get_client_async() as client:
                response = client.pause_cluster(ClusterIdentifier=cluster_identifier)
                response = await response
                return response["Cluster"]["ClusterStatus"] if response and response["Cluster"] else None
        except Exception as error:
            print(error)
            raise error

    async def resume_cluster(self, cluster_identifier: str) -> str:
        """
        Connects to the AWS redshift cluster via aiobotocore and
        resume the cluster for the cluster_identifier passed

        :param cluster_identifier: unique identifier of a cluster
        :type cluster_identifier: str
        """
        async with await self.get_client_async() as client:
            try:
                response = client.resume_cluster(ClusterIdentifier=cluster_identifier)
                response = await response
                print("response ", response)
                return response["Cluster"]["ClusterStatus"] if response and response["Cluster"] else None
            except Exception as error:
                print(error)
                raise error
