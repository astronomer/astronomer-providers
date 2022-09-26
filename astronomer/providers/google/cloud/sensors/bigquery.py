"""This module contains Google Big Query sensors."""
import warnings
from datetime import timedelta
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryTableExistenceTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class BigQueryTableExistenceSensorAsync(BigQueryTableExistenceSensor):
    """
    Checks for the existence of a table in Google Big Query.

    :param project_id: The Google cloud project in which to look for the table.
       The connection supplied to the hook must provide
       access to the specified project.
    :param dataset_id: The name of the dataset in which to look for the table.
       storage bucket.
    :param table_id: The name of the table to check the existence of.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
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
    :param polling_interval: The interval in seconds to wait between checks table existence.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        # TODO: Remove once deprecated
        if polling_interval:
            self.poke_interval = polling_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=BigQueryTableExistenceTrigger(
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                project_id=self.project_id,
                poke_interval=self.poke_interval,
                gcp_conn_id=self.gcp_conn_id,
                hook_params={
                    "delegate_to": self.delegate_to,
                    "impersonation_chain": self.impersonation_chain,
                },
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Optional[Dict[str, str]] = None) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        table_uri = f"{self.project_id}:{self.dataset_id}.{self.table_id}"
        self.log.info("Sensor checks existence of table: %s", table_uri)
        if event:
            if event["status"] == "success":
                return event["message"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
