from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.sensors.named_hive_partition import (
    NamedHivePartitionSensor,
)
from airflow.utils.context import Context

from astronomer.providers.apache.hive.triggers.named_hive_partition import (
    NamedHivePartitionTrigger,
)


class NamedHivePartitionSensorAsync(NamedHivePartitionSensor):
    """
    Waits asynchronously for a set of partitions to show up in Hive.

    .. note::
       NamedHivePartitionSensorAsync uses implya library instead of pyhive.
       The sync version of this sensor uses `pyhive <https://github.com/dropbox/PyHive>`_,
       but `pyhive <https://github.com/dropbox/PyHive>`_ is currently unsupported.

       Since we use `implya <https://github.com/cloudera/impyla>`_ library,
       please set the connection to use the port ``10000`` instead of ``9083``.
       For ``auth_mechansim='GSSAPi'`` the ticket renewal happens through command
       ``airflow kerberos`` in
       `worker/trigger <https://airflow.apache.org/docs/apache-airflow/stable/security/kerberos.html>`_.
       Host map entry in worker/trigger for Public address to Private dns address of the hive cluster.

       The library version of hive and hadoop in ``Dockerfile`` should match the remote
       cluster where they are running.

    :param partition_names: List of fully qualified names of the
        partitions to wait for. A fully qualified name is of the
        form ``schema.table/pk1=pv1/pk2=pv2``, for example,
        default.users/ds=2016-01-01.
    :param metastore_conn_id: Metastore thrift service connection id.
    """

    def execute(self, context: "Context") -> None:
        """Submit a job to Hive and defer"""
        if not self.partition_names:
            raise ValueError("Partition array can't be empty")
        self.defer(
            timeout=self.execution_timeout,
            trigger=NamedHivePartitionTrigger(
                partition_names=self.partition_names,
                metastore_conn_id=self.metastore_conn_id,
                polling_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event: Optional[Dict[str, str]] = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "success":
                self.log.info(event["message"])
            else:
                raise AirflowException(event["message"])
        else:
            raise AirflowException("No event received in trigger callback")
