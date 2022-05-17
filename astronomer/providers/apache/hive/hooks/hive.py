"""This module contains the Apache HiveCli hook async."""
import asyncio
from typing import Tuple

from airflow.configuration import conf
from airflow.hooks.base import BaseHook
from impala.dbapi import connect
from impala.hiveserver2 import HiveServer2Connection


class HiveCliHookAsync(BaseHook):
    """
    HiveCliHookAsync to interact with the Hive using impyla library

    :param metastore_conn_id: connection string for the hive
    :param auth_mechanism: auth mechanism to use for authentication
    """

    def __init__(self, metastore_conn_id: str) -> None:
        """Get the connection parameters separated from connection string"""
        super().__init__()
        self.conn = self.get_connection(conn_id=metastore_conn_id)
        self.auth_mechanism = self.conn.extra_dejson.get("authMechanism", "PLAIN")

    def get_hive_client(self) -> HiveServer2Connection:
        """Makes a connection to the hive client using impyla library"""
        if conf.get("core", "security") == "kerberos":
            auth_mechanism = self.conn.extra_dejson.get("authMechanism", "GSSAPI")
            kerberos_service_name = self.conn.extra_dejson.get("kerberos_service_name", "hive")
            return connect(
                host=self.conn.host,
                port=self.conn.port,
                auth_mechanism=auth_mechanism,
                kerberos_service_name=kerberos_service_name,
            )

        return connect(
            host=self.conn.host,
            port=self.conn.port,
            auth_mechanism=self.auth_mechanism,
            user=self.conn.login,
            password=self.conn.password,
        )

    async def partition_exists(self, table: str, schema: str, partition: str, polling_interval: float) -> str:
        """
        Checks for the existence of a partition in the given hive table.

        :param table: table in hive where the partition exists.
        :param schema: database where the hive table exists
        :param partition: partition to check for in given hive database and hive table.
        :param polling_interval: polling interval in seconds to sleep between checks
        """
        client = self.get_hive_client()
        cursor = client.cursor()
        query = f"show partitions {schema}.{table} partition({partition})"
        cursor.execute_async(query)
        while cursor.is_executing():
            await asyncio.sleep(polling_interval)
        results = cursor.fetchall()
        if len(results) == 0:
            return "failure"
        return "success"

    @staticmethod
    def parse_partition_name(partition: str) -> Tuple[str, str, str]:
        """Parse partition string into schema, table, and partition."""
        first_split = partition.split(".", 1)
        if len(first_split) == 1:
            schema = "default"
            table_partition = max(first_split)  # poor man first
        else:
            schema, table_partition = first_split
        second_split = table_partition.split("/", 1)
        if len(second_split) == 1:
            raise ValueError(f"Could not parse {partition} into table, partition")
        else:
            table, partition = second_split
        return schema, table, partition

    def check_partition_exists(self, schema: str, table: str, partition: str) -> bool:
        """
        Check whether given partition exist or not.

        :param schema: Name of the Hive schema.
        :param table: Name of the table.
        :param partition: Name of the partition
        """
        self.log.info("Checking for partition %s.%s/%s", schema, table, partition)
        client = self.get_hive_client()
        cursor = client.cursor()
        query = f"show partitions {schema}.{table} partition({partition})"
        cursor.execute_async(query)
        results = cursor.fetchall()
        if not results:
            return False
        return True
