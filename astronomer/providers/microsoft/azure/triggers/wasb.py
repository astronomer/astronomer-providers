import asyncio
import warnings
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.microsoft.azure.hooks.wasb import WasbHookAsync


class WasbBlobSensorTrigger(BaseTrigger):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.microsoft.azure.triggers.wasb.WasbBlobSensorTrigger` instead.
    """

    def __init__(
        self,
        container_name: str,
        blob_name: str,
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
        poke_interval: float = 5.0,
    ):
        warnings.warn(
            (
                "This class is deprecated and will be removed in 2.0.0."
                "Use :class: `~airflow.providers.microsoft.azure.triggers.wasb.WasbBlobSensorTrigger` instead"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__()
        self.container_name = container_name
        self.blob_name = blob_name
        self.wasb_conn_id = wasb_conn_id
        self.poke_interval = poke_interval
        self.public_read = public_read

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes WasbBlobSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.microsoft.azure.triggers.wasb.WasbBlobSensorTrigger",
            {
                "container_name": self.container_name,
                "blob_name": self.blob_name,
                "wasb_conn_id": self.wasb_conn_id,
                "poke_interval": self.poke_interval,
                "public_read": self.public_read,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """Makes async connection to Azure WASB and polls for existence of the given blob name."""
        blob_exists = False
        hook = WasbHookAsync(wasb_conn_id=self.wasb_conn_id, public_read=self.public_read)
        try:
            async with hook.blob_service_client:
                while not blob_exists:
                    blob_exists = await hook.check_for_blob_async(
                        container_name=self.container_name,
                        blob_name=self.blob_name,
                    )
                    if blob_exists:
                        message = f"Blob {self.blob_name} found in container {self.container_name}."
                        yield TriggerEvent({"status": "success", "message": message})
                    else:
                        message = (
                            f"Blob {self.blob_name} not available yet in container {self.container_name}."
                            f" Sleeping for {self.poke_interval} seconds"
                        )
                        self.log.info(message)
                        await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class WasbPrefixSensorTrigger(BaseTrigger):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.microsoft.azure.triggers.wasb.WasbPrefixSensorTrigger` instead.
    """

    def __init__(
        self,
        container_name: str,
        prefix: str,
        include: Optional[List[str]] = None,
        delimiter: Optional[str] = "/",
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
        poke_interval: float = 5.0,
    ):
        warnings.warn(
            (
                "This class is deprecated and will be removed in 2.0.0."
                "Use :class: `~airflow.providers.microsoft.azure.triggers.wasb.WasbPrefixSensorTrigger` instead"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__()
        self.container_name = container_name
        self.prefix = prefix
        self.include = include
        self.delimiter = delimiter
        self.wasb_conn_id = wasb_conn_id
        self.poke_interval = poke_interval
        self.public_read = public_read

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes WasbPrefixSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.microsoft.azure.triggers.wasb.WasbPrefixSensorTrigger",
            {
                "container_name": self.container_name,
                "prefix": self.prefix,
                "include": self.include,
                "delimiter": self.delimiter,
                "wasb_conn_id": self.wasb_conn_id,
                "poke_interval": self.poke_interval,
                "public_read": self.public_read,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """Makes async connection to Azure WASB and polls for existence of a blob with given prefix."""
        prefix_exists = False
        hook = WasbHookAsync(wasb_conn_id=self.wasb_conn_id, public_read=self.public_read)
        try:
            async with hook.blob_service_client:
                while not prefix_exists:
                    prefix_exists = await hook.check_for_prefix_async(
                        container_name=self.container_name,
                        prefix=self.prefix,
                        include=self.include,
                        delimiter=self.delimiter,
                    )
                    if prefix_exists:
                        message = f"Prefix {self.prefix} found in container {self.container_name}."
                        yield TriggerEvent({"status": "success", "message": message})
                    else:
                        message = (
                            f"Prefix {self.prefix} not available yet in container {self.container_name}."
                            f" Sleeping for {self.poke_interval} seconds"
                        )
                        self.log.info(message)
                        await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
