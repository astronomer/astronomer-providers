import asyncio
import time
from typing import Any, AsyncIterator, Dict, Optional, Sequence, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from google.api_core.exceptions import NotFound
from google.cloud.dataproc_v1 import Cluster
from google.cloud.dataproc_v1.types import JobStatus, clusters

from astronomer.providers.google.cloud.hooks.dataproc import DataprocHookAsync


class DataprocCreateClusterTrigger(BaseTrigger):
    """
    Asynchronously check the status of a cluster

    :param project_id: The ID of the Google Cloud project the cluster belongs to
    :param region: The Cloud Dataproc region in which to handle the request
    :param cluster_name: The name of the cluster
    :param end_time: Time in second left to check the cluster status
    :param metadata: Additional metadata that is provided to the method
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :param polling_interval: Time in seconds to sleep between checks of cluster status
    """

    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        region: Optional[str] = None,
        cluster_name: str,
        end_time: float,
        metadata: Sequence[Tuple[str, str]] = (),
        delete_on_error: bool = True,
        cluster_config: Optional[Union[Dict[str, Any], clusters.Cluster]] = None,
        labels: Optional[Dict[str, str]] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.end_time = end_time
        self.metadata = metadata
        self.delete_on_error = delete_on_error
        self.cluster_config = cluster_config
        self.labels = labels
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_interval = polling_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes DataprocCreateClusterTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger",
            {
                "project_id": self.project_id,
                "region": self.region,
                "cluster_name": self.cluster_name,
                "end_time": self.end_time,
                "metadata": self.metadata,
                "delete_on_error": self.delete_on_error,
                "cluster_config": self.cluster_config,
                "labels": self.labels,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval": self.polling_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Check the status of cluster until reach the terminal state"""
        while self.end_time > time.time():
            try:
                cluster = await self._get_cluster()
                if cluster.status.state == cluster.status.State.RUNNING:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "data": Cluster.to_dict(cluster),
                            "cluster_name": self.cluster_name,
                        }
                    )
                elif cluster.status.state == cluster.status.State.DELETING:
                    await self._wait_for_deleting()
                    self._create_cluster()
                    await self._handle_error(cluster)

                self.log.info(
                    "Cluster status is %s. Sleeping for %s seconds.",
                    cluster.status.state,
                    self.polling_interval,
                )
                await asyncio.sleep(self.polling_interval)
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})

        yield TriggerEvent({"status": "error", "message": "Timeout"})

    async def _handle_error(self, cluster: clusters.Cluster) -> None:
        if cluster.status.state != cluster.status.State.ERROR:
            return
        self.log.info("Cluster is in ERROR state")
        gcs_uri = self._diagnose_cluster()
        self.log.info("Diagnostic information for cluster %s available at: %s", self.cluster_name, gcs_uri)
        if self.delete_on_error:
            self._delete_cluster()
            await self._wait_for_deleting()
            raise AirflowException(
                "Cluster was created but was in ERROR state. \n"
                " Diagnostic information for cluster %s available at: %s",
                self.cluster_name,
                gcs_uri,
            )
        raise AirflowException(
            "Cluster was created but is in ERROR state. \n "
            "Diagnostic information for cluster %s available at: %s",
            self.cluster_name,
            gcs_uri,
        )

    def _delete_cluster(self) -> None:
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        hook.delete_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            metadata=self.metadata,
        )

    async def _wait_for_deleting(self) -> None:
        while self.end_time > time.time():
            try:
                cluster = await self._get_cluster()
                if cluster.status.State.DELETING:
                    self.log.info(
                        "Cluster status is %s. Sleeping for %s seconds.",
                        cluster.status.state,
                        self.polling_interval,
                    )
                    await asyncio.sleep(self.polling_interval)
            except NotFound:
                return
            except Exception as e:
                raise e

    def _create_cluster(self) -> Any:
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        return hook.create_cluster(
            region=self.region,
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            cluster_config=self.cluster_config,
            labels=self.labels,
            metadata=self.metadata,
        )

    async def _get_cluster(self) -> clusters.Cluster:
        hook = DataprocHookAsync(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        return await hook.get_cluster(
            region=self.region,  # type: ignore[arg-type]
            cluster_name=self.cluster_name,
            project_id=self.project_id,  # type: ignore[arg-type]
            metadata=self.metadata,
        )

    def _diagnose_cluster(self) -> Any:
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        return hook.diagnose_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            metadata=self.metadata,
        )


class DataprocDeleteClusterTrigger(BaseTrigger):
    """
    Asynchronously check the status of a cluster

    :param cluster_name: The name of the cluster
    :param end_time: Time in second left to check the cluster status
    :param project_id: The ID of the Google Cloud project the cluster belongs to
    :param region: The Cloud Dataproc region in which to handle the request
    :param metadata: Additional metadata that is provided to the method
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :param polling_interval: Time in seconds to sleep between checks of cluster status
    """

    def __init__(
        self,
        cluster_name: str,
        end_time: float,
        project_id: Optional[str] = None,
        region: Optional[str] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.end_time = end_time
        self.project_id = project_id
        self.region = region
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_interval = polling_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes DataprocDeleteClusterTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.dataproc.DataprocDeleteClusterTrigger",
            {
                "cluster_name": self.cluster_name,
                "end_time": self.end_time,
                "project_id": self.project_id,
                "region": self.region,
                "metadata": self.metadata,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval": self.polling_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Wait until cluster is deleted completely"""
        hook = DataprocHookAsync(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        while self.end_time > time.time():
            try:
                cluster = await hook.get_cluster(
                    region=self.region,  # type: ignore[arg-type]
                    cluster_name=self.cluster_name,
                    project_id=self.project_id,  # type: ignore[arg-type]
                    metadata=self.metadata,
                )
                self.log.info(
                    "Cluster status is %s. Sleeping for %s seconds.",
                    cluster.status.state,
                    self.polling_interval,
                )
                await asyncio.sleep(self.polling_interval)
            except NotFound:
                yield TriggerEvent({"status": "success", "message": ""})
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})
        yield TriggerEvent({"status": "error", "message": "Timeout"})


class DataProcSubmitTrigger(BaseTrigger):
    """
    Check for the state of a previously submitted Dataproc job.

    :param dataproc_job_id: The Dataproc job ID to poll. (templated)
    :param region: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :param wait_timeout: How many seconds wait for job to be ready.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        *,
        dataproc_job_id: str,
        region: Optional[str] = None,
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        polling_interval: float = 5.0,
    ) -> None:
        super().__init__()
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataproc_job_id = dataproc_job_id
        self.region = region
        self.impersonation_chain = impersonation_chain
        self.polling_interval = polling_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes DataProcSubmitTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger",
            {
                "project_id": self.project_id,
                "dataproc_job_id": self.dataproc_job_id,
                "region": self.region,
                "polling_interval": self.polling_interval,
                "impersonation_chain": self.impersonation_chain,
                "gcp_conn_id": self.gcp_conn_id,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the job running on Google Cloud DataProc is completed or not"""
        try:
            hook = DataprocHookAsync(
                gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
            )
            while True:
                job_status = await self._get_job_status(hook)
                if "status" in job_status and job_status["status"] == "success":
                    yield TriggerEvent(job_status)
                elif "status" in job_status and job_status["status"] == "error":
                    yield TriggerEvent(job_status)
                await asyncio.sleep(self.polling_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    async def _get_job_status(self, hook: DataprocHookAsync) -> Dict[str, str]:
        """Gets the status of the given job_id from the Google Cloud DataProc"""
        job = await hook.get_job(job_id=self.dataproc_job_id, region=self.region, project_id=self.project_id)
        state = job.status.state
        if state == JobStatus.State.ERROR:
            return {"status": "error", "message": "Job Failed", "job_id": self.dataproc_job_id}
        elif state in {
            JobStatus.State.CANCELLED,
            JobStatus.State.CANCEL_PENDING,
            JobStatus.State.CANCEL_STARTED,
        }:
            return {"status": "error", "message": "Job got cancelled", "job_id": self.dataproc_job_id}
        elif JobStatus.State.DONE == state:
            return {
                "status": "success",
                "message": "Job completed successfully",
                "job_id": self.dataproc_job_id,
            }
        elif JobStatus.State.ATTEMPT_FAILURE == state:
            return {"status": "pending", "message": "Job is in pending state", "job_id": self.dataproc_job_id}
        return {"status": "pending", "message": "Job is in pending state", "job_id": self.dataproc_job_id}
