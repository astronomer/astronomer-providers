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
        polling_period_seconds: float,
        metastore_conn_id: str,
        schema: str,
    ):
        super().__init__()
        self.table = table
        self.partition = partition
        self.polling_period_seconds = polling_period_seconds
        self.metastore_conn_id: str = metastore_conn_id
        self.schema = schema

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes HivePartitionTrigger arguments and classpath."""
        return (
            "astronomer.providers.apache.hive.triggers.hive_partition.HivePartitionTrigger",
            {
                "table": self.table,
                "partition": self.partition,
                "polling_period_seconds": self.polling_period_seconds,
                "metastore_conn_id": self.metastore_conn_id,
                "schema": self.schema,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the relevant table partition is present in the table or wait for it."""
        try:
            hook = self._get_async_hook()
            while True:
                res = await self._partition_exists(
                    hook=hook, table=self.table, schema=self.schema, partition=self.partition
                )
                if res == "success":
                    yield TriggerEvent({"status": "success", "message": res})
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    def _get_async_hook(self) -> HiveCliHookAsync:
        return HiveCliHookAsync(conn_id=self.metastore_conn_id)

    async def _partition_exists(self, hook: HiveCliHookAsync, table: str, schema: str, partition: str) -> str:
        """
        Checks for the existence of a partition in the given hive table.

        :param table: The Google Cloud Storage bucket where the object is.
        :param schema: The name of the blob_name to check in the Google cloud
            storage bucket.
        :param partition: The name of the blob_name to check in the Google cloud
            storage bucket.
        """
        client = hook.get_hive_client()
        cursor = client.cursor()
        query = f"show partitions {schema}.{table} partition({partition})"
        cursor.execute_async(query)
        while cursor.is_executing():
            asyncio.sleep(self.polling_period_seconds)
        results = cursor.fetchall()
        if len(results) == 0:
            return "failure"
        return "success"
