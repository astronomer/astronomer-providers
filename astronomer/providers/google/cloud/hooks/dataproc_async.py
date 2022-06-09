import warnings
from abc import ABC
from typing import Optional, Sequence, Tuple

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import ClusterControllerAsyncClient


class DataprocHookAsync(GoogleBaseHook, ABC):
    """Async Hook for Google Cloud Dataproc APIs"""

    def get_cluster_client(
        self, region: Optional[str] = None, location: Optional[str] = None
    ) -> ClusterControllerAsyncClient:
        """
        Return ClusterControllerAsyncClient

        :param region: The Cloud Dataproc region in which to handle the request.
        :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request.
        """
        if location is not None:
            warnings.warn(
                "Parameter `location` will be deprecated. "
                "Please provide value through `region` parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            region = location
        client_options = None
        if region and region != "global":
            client_options = {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}

        return ClusterControllerAsyncClient(
            credentials=self._get_credentials(), client_info=CLIENT_INFO, client_options=client_options
        )

    async def get_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ):
        """
        Get a cluster details

        :param region: The Cloud Dataproc region in which to handle the request
        :param cluster_name: The name of the cluster
        :param project_id:  The ID of the Google Cloud project the cluster belongs to
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt
        :param metadata: Additional metadata that is provided to the method
        """
        client = self.get_cluster_client(region=region)
        return await client.get_cluster(
            region=region,
            cluster_name=cluster_name,
            project_id=project_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
