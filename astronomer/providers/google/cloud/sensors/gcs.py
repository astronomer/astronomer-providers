"""This module contains Google Cloud Storage sensors."""
import warnings
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)

from astronomer.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSCheckBlobUpdateTimeTrigger,
    GCSPrefixBlobTrigger,
    GCSUploadSessionTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class GCSObjectExistenceSensorAsync(GCSObjectExistenceSensor):
    """
    Checks for the existence of a file in Google Cloud Storage.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param object: The name of the object to check in the Google cloud storage bucket.
    :param google_cloud_conn_id: The connection ID to use when connecting to Google Cloud Storage.
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

    def __init__(
        self,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            self.poke_interval = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)

    def execute(self, context: "Context") -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=GCSBlobTrigger(
                bucket=self.bucket,
                object_name=self.object,
                poke_interval=self.poke_interval,
                google_cloud_conn_id=self.google_cloud_conn_id,
                hook_params={
                    "delegate_to": self.delegate_to,
                    "impersonation_chain": self.impersonation_chain,
                },
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Dict[str, str]) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("File %s was found in bucket %s.", self.object, self.bucket)
        return event["message"]


class GCSObjectsWithPrefixExistenceSensorAsync(GCSObjectsWithPrefixExistenceSensor):
    """
    Async Operator that Checks for the existence of GCS objects at a given prefix, passing matches via XCom.

    When files matching the given prefix are found, the poke method's criteria will be
    fulfilled and the matching objects will be returned from the operator and passed
    through XCom for downstream tasks.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param prefix: The name of the prefix to check in the Google cloud storage bucket.
    :param google_cloud_conn_id: The connection ID to use when connecting to Google Cloud Storage.
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
    :param polling_interval: The interval in seconds to wait between checks for matching objects.
    """

    def __init__(
        self,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            self.poke_interval = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:  # type: ignore[override]
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=GCSPrefixBlobTrigger(
                bucket=self.bucket,
                prefix=self.prefix,
                poke_interval=self.poke_interval,
                google_cloud_conn_id=self.google_cloud_conn_id,
                hook_params={
                    "delegate_to": self.delegate_to,
                    "impersonation_chain": self.impersonation_chain,
                },
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Dict[str, Any], event: Dict[str, Union[str, List[str]]]
    ) -> Union[str, List[str]]:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("Sensor checks existence of objects: %s, %s", self.bucket, self.prefix)
        if event["status"] == "success":
            return event["matches"]
        raise AirflowException(event["message"])


class GCSUploadSessionCompleteSensorAsync(GCSUploadSessionCompleteSensor):
    """
    Checks for changes in the number of objects at prefix in Google Cloud Storage
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will no behave correctly
    in reschedule mode, as the state of the listed objects in the GCS bucket will
    be lost between rescheduled invocations.

    :param bucket: The Google Cloud Storage bucket where the objects are expected.
    :param prefix: The name of the prefix to check in the Google cloud storage bucket.
    :param inactivity_period: The total seconds of inactivity to designate
        an upload session is over. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for upload session
        to be considered valid.
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :param google_cloud_conn_id: The connection ID to use when connecting
        to Google Cloud Storage.
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
    :param polling_interval: The interval in seconds to wait between checks for matching objects.
    """

    def __init__(
        self,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            self.poke_interval = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=GCSUploadSessionTrigger(
                bucket=self.bucket,
                prefix=self.prefix,
                poke_interval=self.poke_interval,
                google_cloud_conn_id=self.google_cloud_conn_id,
                inactivity_period=self.inactivity_period,
                min_objects=self.min_objects,
                previous_objects=self.previous_objects,
                allow_delete=self.allow_delete,
                hook_params={
                    "delegate_to": self.delegate_to,
                    "impersonation_chain": self.impersonation_chain,
                },
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[str, str]] = None) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "success":
                return event["message"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")


class GCSObjectUpdateSensorAsync(GCSObjectUpdateSensor):
    """
    Async version to check if an object is updated in Google Cloud Storage

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param object: The name of the object to download in the Google cloud
        storage bucket.
    :param ts_func: Callback for defining the update condition. The default callback
        returns execution_date + schedule_interval. The callback takes the context
        as parameter.
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
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

    def __init__(
        self,
        polling_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        # TODO: Remove once deprecated
        if polling_interval:
            self.poke_interval = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=GCSCheckBlobUpdateTimeTrigger(
                bucket=self.bucket,
                object_name=self.object,
                ts=self.ts_func(context),
                poke_interval=self.poke_interval,
                google_cloud_conn_id=self.google_cloud_conn_id,
                hook_params={
                    "delegate_to": self.delegate_to,
                    "impersonation_chain": self.impersonation_chain,
                },
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[str, str]] = None) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "success":
                self.log.info(
                    "Sensor checks update time for object %s in bucket : %s", self.object, self.bucket
                )
                return event["message"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
