import asyncio
from typing import Any, AsyncIterator, Dict, List, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.apache.hive.hooks.hive import HiveCliHookAsync


class NamedHivePartitionTrigger(BaseTrigger):
    """
    A trigger that fires, and it looks for a partition in the given table
    in the database or wait for the partition.

    :param partition_names: List of fully qualified names of the
        partitions to wait for.
    :param metastore_conn_id: connection string to connect to hive.
    :param polling_interval: polling period in seconds to check for the partition.
    """

    def __init__(
        self,
        partition_names: List[str],
        metastore_conn_id: str,
        polling_interval: float,
    ):
        super().__init__()
        self.partition_names = partition_names
        self.polling_interval = polling_interval
        self.metastore_conn_id: str = metastore_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes NamedHivePartitionTrigger arguments and classpath."""
        return (
            "astronomer.providers.apache.hive.triggers.named_hive_partition.NamedHivePartitionTrigger",
            {
                "partition_names": self.partition_names,
                "polling_interval": self.polling_interval,
                "metastore_conn_id": self.metastore_conn_id,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Run until found all given partition in Hive."""
        try:
            hook = HiveCliHookAsync(metastore_conn_id=self.metastore_conn_id)
            number_of_partitions = len(self.partition_names)
            res = [False] * number_of_partitions
            while True:
                for i in range(number_of_partitions):
                    if not res[i]:
                        schema, table, partition = hook.parse_partition_name(self.partition_names[i])
                        if hook.check_partition_exists(
                            schema=schema,
                            table=table,
                            partition=partition,
                        ):
                            self.log.info("Found partition for %s.%s/%s", schema, table, partition)
                            res[i] = True
                    if all(res):
                        yield TriggerEvent({"status": "success", "message": "Named hive partition found."})
                await asyncio.sleep(self.polling_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
