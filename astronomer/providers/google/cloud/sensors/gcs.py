"""This module contains Google Cloud Storage sensors."""
from __future__ import annotations

import warnings
from datetime import timedelta
from typing import Any

from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)

from astronomer.providers.google.cloud.triggers.gcs import (
    GCSCheckBlobUpdateTimeTrigger,
)
from astronomer.providers.utils.sensor_util import poke, raise_error_or_skip_exception
from astronomer.providers.utils.typing_compat import Context


class GCSObjectExistenceSensorAsync(GCSObjectExistenceSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
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

        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)


class GCSObjectsWithPrefixExistenceSensorAsync(GCSObjectsWithPrefixExistenceSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
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
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSObjectsWithPrefixExistenceSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)

class GCSUploadSessionCompleteSensorAsync(GCSUploadSessionCompleteSensor):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *args: Any,
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

        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.google.cloud.sensors.gcs.GCSUploadSessionCompleteSensor`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)


class GCSObjectUpdateSensorAsync(GCSObjectUpdateSensor):
    """
    Async version to check if an object is updated in Google Cloud Storage

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param object: The name of the object to download in the Google cloud
        storage bucket.
    :param ts_func: Callback for defining the update condition. The default callback
        returns execution_date + schedule. The callback takes the context
        as parameter.
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :param delegate_to: (Removed in apache-airflow-providers-google release 10.0.0, use impersonation_chain instead)
        The account to impersonate using domain-wide delegation of authority, if any. For this to work, the service
        account making the request must have domain-wide delegation enabled.
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
        hook_params = {"impersonation_chain": self.impersonation_chain}
        if hasattr(self, "delegate_to"):
            hook_params["delegate_to"] = self.delegate_to

        if not poke(self, context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=GCSCheckBlobUpdateTimeTrigger(
                    bucket=self.bucket,
                    object_name=self.object,
                    ts=self.ts_func(context),
                    poke_interval=self.poke_interval,
                    google_cloud_conn_id=self.google_cloud_conn_id,
                    hook_params=hook_params,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: dict[str, Any], event: dict[str, str] | None = None) -> str:  # type: ignore[return]
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
            raise_error_or_skip_exception(self.soft_fail, event["message"])
