"""This module contains Google Dataproc operators."""
from __future__ import annotations

import time
import warnings
from typing import Any

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
from google.api_core.exceptions import NotFound

from astronomer.providers.google.cloud.triggers.dataproc import (
    DataprocCreateClusterTrigger,
    DataprocDeleteClusterTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class DataprocCreateClusterOperatorAsync(DataprocCreateClusterOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use `airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        kwargs["polling_interval_seconds"] = polling_interval
        super().__init__(deferrable=True, **kwargs)


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

        try:
            hook.get_cluster(project_id=self.project_id, region=self.region, cluster_name=self.cluster_name)
        except NotFound:
            self.log.info("Cluster deleted.")
            return
        except Exception as e:
            raise AirflowException(str(e))

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

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> Any:
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
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)


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

    def execute(self, context: Context) -> None:
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
        cluster = hook.get_cluster(
            project_id=self.project_id, region=self.region, cluster_name=self.cluster_name
        )
        if cluster.status.state == cluster.status.State.RUNNING:
            self.log.info("Updated %s cluster.", self.cluster_name)
        else:
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

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
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
