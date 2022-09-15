import warnings
from datetime import timedelta
from typing import Any, Dict, List, Optional

from airflow import AirflowException
from airflow.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensor,
    WasbPrefixSensor,
)

from astronomer.providers.microsoft.azure.triggers.wasb import (
    WasbBlobSensorTrigger,
    WasbPrefixSensorTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class WasbBlobSensorAsync(WasbBlobSensor):
    """
    Polls asynchronously for the existence of a blob in a WASB container.

    :param container_name: name of the container in which the blob should be searched for
    :param blob_name: name of the blob to check existence for
    :param wasb_conn_id: the connection identifier for connecting to Azure WASB
    :param poll_interval:  polling period in seconds to check for the status
    :param public_read: whether an anonymous public read access should be used. Default is False
    """

    def __init__(
        self,
        *,
        container_name: str,
        blob_name: str,
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
        poll_interval: float = 5.0,
        **kwargs: Any,
    ):
        self.container_name = container_name
        self.blob_name = blob_name
        # TODO: Remove once deprecated
        if poll_interval:
            self.poke_interval = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(container_name=container_name, blob_name=blob_name, **kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.public_read = public_read

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=WasbBlobSensorTrigger(
                container_name=self.container_name,
                blob_name=self.blob_name,
                wasb_conn_id=self.wasb_conn_id,
                public_read=self.public_read,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, str]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        else:
            raise AirflowException("Did not receive valid event from the triggerer")


class WasbPrefixSensorAsync(WasbPrefixSensor):
    """
     Polls asynchronously for the existence of a blob having the given prefix in a WASB container.

    :param container_name: name of the container in which the blob should be searched for
    :param blob_name: name of the blob to check existence for
    :param include: specifies one or more additional datasets to include in the
            response. Options include: ``snapshots``, ``metadata``, ``uncommittedblobs``,
            ``copy`, ``deleted``
    :param delimiter: filters objects based on the delimiter (for e.g '.csv')
    :param wasb_conn_id: the connection identifier for connecting to Azure WASB
    :param poll_interval:  polling period in seconds to check for the status
    :param public_read: whether an anonymous public read access should be used. Default is False
    """

    def __init__(
        self,
        *,
        container_name: str,
        prefix: str,
        include: Optional[List[str]] = None,
        delimiter: Optional[str] = "/",
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
        poll_interval: float = 5.0,
        **kwargs: Any,
    ):
        # TODO: Remove once deprecated
        if poll_interval:
            self.poke_interval = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(container_name=container_name, prefix=prefix, **kwargs)
        self.container_name = container_name
        self.prefix = prefix
        self.include = include
        self.delimiter = delimiter
        self.wasb_conn_id = wasb_conn_id
        self.public_read = public_read

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=WasbPrefixSensorTrigger(
                container_name=self.container_name,
                prefix=self.prefix,
                include=self.include,
                delimiter=self.delimiter,
                wasb_conn_id=self.wasb_conn_id,
                public_read=self.public_read,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, str]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        else:
            raise AirflowException("Did not receive valid event from the triggerer")
