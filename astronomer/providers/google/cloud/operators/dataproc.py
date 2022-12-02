"""This module contains Google Dataproc operators."""
import time
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.cloud.links.dataproc import (
    DATAPROC_CLUSTER_LINK,
    DataprocLink,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)
from google.api_core.exceptions import AlreadyExists

from astronomer.providers.google.cloud.triggers.dataproc import (
    DataprocCreateClusterTrigger,
    DataprocDeleteClusterTrigger,
    DataProcSubmitTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class DataprocCreateClusterOperatorAsync(DataprocCreateClusterOperator):
    """
    Create a new cluster on Google Cloud Dataproc Asynchronously.

    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :param cluster_name: Name of the cluster to create
    :param labels: Labels that will be assigned to created cluster
    :param cluster_config: Required. The cluster config to create.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
    :param virtual_cluster_config: Optional. The virtual cluster config, used when creating a Dataproc
        cluster that does not directly control the underlying compute resources, for example, when creating a
        `Dataproc-on-GKE cluster
        <https://cloud.google.com/dataproc/docs/concepts/jobs/dataproc-gke#create-a-dataproc-on-gke-cluster>`
    :param region: The specified region where the dataproc cluster is created.
    :param delete_on_error: If true the cluster will be deleted if created with ERROR state. Default
        value is true.
    :param use_if_exists: If true use existing cluster
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param polling_interval: Time in seconds to sleep between checks of cluster status
    """

    def __init__(
        self,
        *,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.polling_interval = polling_interval

    def execute(self, context: Context) -> None:  # type: ignore[override]
        """Call create cluster API and defer to DataprocCreateClusterTrigger to check the status"""
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        DataprocLink.persist(
            context=context, task_instance=self, url=DATAPROC_CLUSTER_LINK, resource=self.cluster_name
        )
        try:
            hook.create_cluster(
                region=self.region,
                project_id=self.project_id,
                cluster_name=self.cluster_name,
                cluster_config=self.cluster_config,
                labels=self.labels,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
        except AlreadyExists:
            if not self.use_if_exists:
                raise
            self.log.info("Cluster already exists.")

        end_time: float = time.time() + self.timeout

        self.defer(
            trigger=DataprocCreateClusterTrigger(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
                end_time=end_time,
                metadata=self.metadata,
                delete_on_error=self.delete_on_error,
                cluster_config=self.cluster_config,
                labels=self.labels,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                polling_interval=self.polling_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Optional[Dict[str, Any]] = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and event["status"] == "success":
            self.log.info("Cluster created successfully \n %s", event["data"])
            return event["data"]
        elif event and event["status"] == "error":
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")


class DataprocDeleteClusterOperatorAsync(DataprocDeleteClusterOperator):
    """
    Delete a cluster on Google Cloud Dataproc Asynchronously.

    :param region: Required. The Cloud Dataproc region in which to handle the request (templated).
    :param cluster_name: Required. The cluster name (templated).
    :param project_id: Optional. The ID of the Google Cloud project that the cluster belongs to (templated).
    :param cluster_uuid: Optional. Specifying the ``cluster_uuid`` means the RPC should fail
        if cluster with specified UUID does not exist.
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param polling_interval: Time in seconds to sleep between checks of cluster status
    """

    def __init__(
        self,
        *,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.polling_interval = polling_interval
        if self.timeout is None:
            self.timeout: float = 24 * 60 * 60

    def execute(self, context: Context) -> None:
        """Call delete cluster API and defer to wait for cluster to completely deleted"""
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Deleting cluster: %s", self.cluster_name)
        hook.delete_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            cluster_uuid=self.cluster_uuid,
            request_id=self.request_id,
            retry=self.retry,
            metadata=self.metadata,
        )

        end_time: float = time.time() + self.timeout

        self.defer(
            trigger=DataprocDeleteClusterTrigger(
                gcp_conn_id=self.gcp_conn_id,
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
                request_id=self.request_id,
                retry=self.retry,
                end_time=end_time,
                metadata=self.metadata,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Optional[Dict[str, Any]] = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        elif event is None:
            raise AirflowException("No event received in trigger callback")
        self.log.info("Cluster deleted.")


class DataprocSubmitJobOperatorAsync(DataprocSubmitJobOperator):
    """
    Submits a job to a cluster and wait until is completely finished or any error occurs.

    :param project_id: Optional. The ID of the Google Cloud project that the job belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param job: Required. The job resource.
        If a dict is provided, it must be of the same form as the protobuf message
        class:`~google.cloud.dataproc_v1.types.Job`
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
        ``Job`` created and stored in the backend is returned.
        It is recommended to always set this value to a UUID.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    def execute(self, context: Context) -> None:
        """
        Airflow runs this method on the worker and defers using the trigger.
        Submit the job and get the job_id using which we defer and poll in trigger
        """
        self.log.info("Submitting job \n %s", self.job)
        self.hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        job_object = self.hook.submit_job(
            project_id=self.project_id,
            region=self.region,
            job=self.job,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        job_id = job_object.reference.job_id
        self.log.info("Job %s submitted successfully.", job_id)
        self.job_id = job_id
        self.defer(
            timeout=self.execution_timeout,
            trigger=DataProcSubmitTrigger(
                gcp_conn_id=self.gcp_conn_id,
                dataproc_job_id=job_id,
                project_id=self.project_id,
                region=self.region,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(  # type: ignore[override]
        self, context: Context, event: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "success":
                self.log.info("Job %s completed successfully.", event["job_id"])
                return event["job_id"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")


class DataprocUpdateClusterOperatorAsync(DataprocUpdateClusterOperator):
    """
    Updates an existing cluster in a Google cloud platform project.

    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param project_id: Optional. The ID of the Google Cloud project the cluster belongs to.
    :param cluster_name: Required. The cluster name.
    :param cluster: Required. The changes to the cluster.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1.types.Cluster`
    :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
        example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
        specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify the
        new value. If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decommissioning. Graceful
        decommissioning allows removing nodes from the cluster without interrupting jobs in progress. Timeout
        specifies how long to wait for jobs in progress to finish before forcefully removing nodes (and
        potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the maximum
        allowed timeout is 1 day.
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param polling_interval: Time in seconds to sleep between checks of cluster status
    """

    def __init__(
        self,
        *,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.polling_interval = polling_interval
        if self.timeout is None:
            self.timeout: float = 24 * 60 * 60

    def execute(self, context: "Context") -> None:
        """Call update cluster API and defer to wait for cluster update to complete"""
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        # Save data required by extra links no matter what the cluster status will be
        DataprocLink.persist(
            context=context, task_instance=self, url=DATAPROC_CLUSTER_LINK, resource=self.cluster_name
        )
        self.log.info("Updating %s cluster.", self.cluster_name)
        hook.update_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            cluster=self.cluster,
            update_mask=self.update_mask,
            graceful_decommission_timeout=self.graceful_decommission_timeout,
            request_id=self.request_id,
            retry=self.retry,
            metadata=self.metadata,
        )

        end_time: float = time.time() + self.timeout

        self.defer(
            trigger=DataprocCreateClusterTrigger(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
                end_time=end_time,
                metadata=self.metadata,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                polling_interval=self.polling_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[str, Any]] = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and event["status"] == "success":
            self.log.info("Updated %s cluster.", event["cluster_name"])
            return
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
