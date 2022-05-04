from typing import Any, Dict, Optional

from airflow import AirflowException
from airflow.providers.apache.hive.sensors.hive_partition import HivePartitionSensor
from airflow.utils.context import Context

from astronomer.providers.apache.hive.triggers.hive_partition import (
    HivePartitionTrigger,
)


class HivePartitionSensorAsync(HivePartitionSensor):
    """
    Waits for a given partition to show up in Hive table asynchronously.

    .. note::
       HivePartitionSensorAsync uses implya library instead of pyhive.
       The sync version of this sensor uses `pyhive <https://github.com/dropbox/PyHive>`_,
       but `pyhive <https://github.com/dropbox/PyHive>`_ is currently unsupported.

       Since we use `implya <https://github.com/cloudera/impyla>`_ library,
       please set the connection to use the port ``10000`` instead of ``9083``.
       This sensor currently supports ``auth_mechansim='PLAIN'`` only.

       The library version of hive and hadoop in ``Dockerfile`` should match the remote
       cluster where they are running.

    :param table: the table where the partition is present.
    :param partition: The partition clause to wait for. This is passed as
        notation as in "ds='2015-01-01'"
    :param schema: database which needs to be connected in hive. By default it is 'default'
    :param metastore_conn_id: connection string to connect to hive.
    :param polling_interval: The interval in seconds to wait between checks for partition.
    """

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=self.execution_timeout,
            trigger=HivePartitionTrigger(
                table=self.table,
                schema=self.schema,
                partition=self.partition,
                polling_interval=self.poke_interval,
                metastore_conn_id=self.metastore_conn_id,
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
                    "Success criteria met. Found partition %s in table: %s", self.partition, self.table
                )
                return event["message"]
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
