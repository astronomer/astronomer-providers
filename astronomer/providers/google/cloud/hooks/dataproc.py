import warnings
from typing import Any, Optional, Sequence, Tuple, Union

from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.api_core import gapic_v1
from google.api_core.client_options import ClientOptions
from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import (
    ClusterControllerAsyncClient,
    Job,
    JobControllerAsyncClient,
)
from google.cloud.dataproc_v1.types import clusters

JobType = Union[Job, Any]


class DataprocHookAsync(GoogleBaseHook):
    """Async Hook for Google Cloud Dataproc APIs"""

    def get_cluster_client(
        self, region: Optional[str] = None, location: Optional[str] = None
    ) -> ClusterControllerAsyncClient:
        """
        Get async cluster controller client for GCP Dataproc.

        :param region: The Cloud Dataproc region in which to handle the request.
        :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request.
        """
        client_options, region = self._get_client_options_and_region(region=region, location=location)
        try:
            # for apache-airflow-providers-google<8.4.0
            credentials = self._get_credentials()  # type: ignore[attr-defined]
        except AttributeError:  # pragma: no cover
            # for apache-airflow-providers-google>=8.4.0
            credentials = self.get_credentials()
        return ClusterControllerAsyncClient(
            credentials=credentials,
            client_info=CLIENT_INFO,
            client_options=client_options,
        )

    def get_job_client(
        self, region: Optional[str] = None, location: Optional[str] = None
    ) -> JobControllerAsyncClient:
        """
        Get async job controller for GCP Dataproc.

        :param region: The Cloud Dataproc region in which to handle the request.
        :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request.
        """
        client_options, region = self._get_client_options_and_region(region=region, location=location)
        try:
            # for apache-airflow-providers-google<8.4.0
            credentials = self._get_credentials()  # type: ignore[attr-defined]
        except AttributeError:  # pragma: no cover
            # for apache-airflow-providers-google>=8.4.0
            credentials = self.get_credentials()
        return JobControllerAsyncClient(
            credentials=credentials,
            client_info=CLIENT_INFO,
            client_options=client_options,
        )

    async def get_cluster(
        self,
        region: str,
        cluster_name: str,
        project_id: str,
        retry: Union[Retry, gapic_v1.method._MethodDefault] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> clusters.Cluster:
        """
        Get a cluster details from GCP using `ClusterControllerAsyncClient`

        :param region: The Cloud Dataproc region in which to handle the request
        :param cluster_name: The name of the cluster
        :param project_id:  The ID of the Google Cloud project the cluster belongs to
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried
        :param metadata: Additional metadata that is provided to the method
        """
        client = self.get_cluster_client(region=region)
        return await client.get_cluster(
            region=region,
            cluster_name=cluster_name,
            project_id=project_id,
            retry=retry,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_job(
        self,
        job_id: str,
        project_id: str,
        timeout: float = 5,
        region: Optional[str] = None,
        location: Optional[str] = None,
        retry: Union[Retry, gapic_v1.method._MethodDefault] = gapic_v1.method.DEFAULT,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> JobType:
        """
        Gets the resource representation for a job using `JobControllerAsyncClient`.

        :param job_id: Id of the Dataproc job
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        client = self.get_job_client(region=region, location=location)
        job = await client.get_job(
            request={"project_id": project_id, "region": region, "job_id": job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job

    def _get_client_options_and_region(
        self, region: Optional[str] = None, location: Optional[str] = None
    ) -> Tuple[ClientOptions, Optional[str]]:
        """
        Checks for location if present or not and creates a client options using the provided region/location

        :param region: Required. The Cloud Dataproc region in which to handle the request.
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
        client_options = ClientOptions()
        if region and region != "global":
            client_options = ClientOptions(api_endpoint=f"{region}-dataproc.googleapis.com:443")
        return client_options, region
