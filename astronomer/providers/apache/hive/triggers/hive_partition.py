import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.apache.hive.hooks.hive import HiveCliHookAsync


class HivePartitionTrigger(BaseTrigger):
    """
    A trigger that fires and it looks for a partition in the given table
    in the database or wait for the partition.

    :param table: the table where the partition is present.
    :param partition: The partition clause to wait for.
    :param schema: database which needs to be connected in hive.
    :param metastore_conn_id: connection string to connect to hive.
    :param polling_period_seconds: polling period in seconds to check for the partition.
    """

    def __init__(
        self,
        table: str,
        partition: str,
        polling_interval: float,
        metastore_conn_id: str,
        schema: str,
    ):
        super().__init__()
        self.table = table
        self.partition = partition
        self.polling_interval = polling_interval
        self.metastore_conn_id: str = metastore_conn_id
        self.schema = schema

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes HivePartitionTrigger arguments and classpath."""
        return (
            "astronomer.providers.apache.hive.triggers.hive_partition.HivePartitionTrigger",
            {
                "table": self.table,
                "partition": self.partition,
                "polling_interval": self.polling_interval,
                "metastore_conn_id": self.metastore_conn_id,
                "schema": self.schema,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the relevant table partition is present in the table or wait for it."""
        try:
            hook = self._get_async_hook()
            while True:
                res = await hook.partition_exists(
                    table=self.table,
                    schema=self.schema,
                    partition=self.partition,
                    polling_interval=self.polling_interval,
                )
                if res == "success":
                    yield TriggerEvent({"status": "success", "message": res})
                await asyncio.sleep(self.polling_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> HiveCliHookAsync:
        return HiveCliHookAsync(metastore_conn_id=self.metastore_conn_id)
