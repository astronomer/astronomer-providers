"""This module contains Google Cloud Storage sensors."""

from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor

from astronomer.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSCheckBlobUpdateTimeTrigger,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GCSObjectExistenceSensorAsync(BaseOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.
    :param bucket: The Google Cloud Storage bucket where the object is.
    :param object: The name of the object to check in the Google cloud
        storage bucket.
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :param bucket: The bucket name where the objects in GCS will be present
    :param object: the object name of the file or folder present in the google
          cloud storage
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("bucket", "object", "impersonation_chain")
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        bucket: str,
        object: str,
        polling_interval: float = 5.0,
        google_cloud_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.object = object
        self.polling_interval = polling_interval
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context"):
        self.defer(
            timeout=self.execution_timeout,
            trigger=GCSBlobTrigger(
                bucket=self.bucket,
                object_name=self.object,
                polling_period_seconds=self.polling_interval,
                google_cloud_conn_id=self.google_cloud_conn_id,
                hook_params=dict(delegate_to=self.delegate_to, impersonation_chain=self.impersonation_chain),
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("File %s was found in bucket %s.", self.object, self.bucket)
        return event["message"]


class GCSObjectUpdateSensorAsync(GCSObjectUpdateSensor):
    """
    Async version to checks if an object is updated in Google Cloud Storage
    """

    def __init__(
        self,
        polling_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.polling_interval = polling_interval

    def execute(self, context: "Context") -> None:
        self.defer(
            timeout=self.execution_timeout,
            trigger=GCSCheckBlobUpdateTimeTrigger(
                bucket=self.bucket,
                object_name=self.object,
                ts=self.ts_func(context),
                polling_period_seconds=self.polling_interval,
                google_cloud_conn_id=self.google_cloud_conn_id,
                hook_params=dict(delegate_to=self.delegate_to, impersonation_chain=self.impersonation_chain),
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: "Context", event: Optional[Dict[Any, Any]] = None
    ) -> Optional[str]:  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info('Sensor checks update time for object %s in bucket : %s', self.object, self.bucket)
        return event["message"]
