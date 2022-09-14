from datetime import timedelta
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.sensors.hive_partition import HivePartitionSensor

from astronomer.providers.apache.hive.triggers.hive_partition import (
    HivePartitionTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class HivePartitionSensorAsync(HivePartitionSensor):
    """
    Waits for a given partition to show up in Hive table asynchronously.

    .. note::
       HivePartitionSensorAsync uses impyla library instead of PyHive.
       The sync version of this sensor uses `PyHive <https://github.com/dropbox/PyHive>`.

       Since we use `impyla <https://github.com/cloudera/impyla>`_ library,
       please set the connection to use the port ``10000`` instead of ``9083``.
       For ``auth_mechanism='GSSAPI'`` the ticket renewal happens through command
       ``airflow kerberos`` in
       `worker/trigger <https://airflow.apache.org/docs/apache-airflow/stable/security/kerberos.html>`_.

       You may also need to allow traffic from Airflow worker/Triggerer to the Hive instance, depending on where
       they are running. For example, you might consider adding an entry in the ``etc/hosts`` file present in the
       Airflow worker/Triggerer, which maps the EMR Master node Public IP Address to its Private DNS Name to
       allow the network traffic.

       The library version of hive and hadoop in ``Dockerfile`` should match the remote
       cluster where they are running.

    :param table: the table where the partition is present.
    :param partition: The partition clause to wait for. This is passed as
        notation as in "ds='2015-01-01'"
    :param schema: database which needs to be connected in hive. By default, it is 'default'
    :param metastore_conn_id: connection string to connect to hive.
    :param polling_interval: The interval in seconds to wait between checks for partition.
    """

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=HivePartitionTrigger(
                table=self.table,
                schema=self.schema,
                partition=self.partition,
                polling_interval=self.poke_interval,
                metastore_conn_id=self.metastore_conn_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Optional[Dict[str, str]] = None) -> str:
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
