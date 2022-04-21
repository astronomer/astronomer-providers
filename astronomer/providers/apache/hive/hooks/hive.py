"""This module contains the Apache HiveCli hook async."""
import asyncio

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
        self.metastore_conn_id = self.get_connection(conn_id=metastore_conn_id)
        self.auth_mechanism = self.metastore_conn_id.extra_dejson.get("authMechanism", "PLAIN")

    def get_hive_client(self) -> HiveServer2Connection:
        """Makes a connection to the hive client using impyla library"""
        return connect(
            host=self.metastore_conn_id.host,
            port=self.metastore_conn_id.port,
            auth_mechanism=self.auth_mechanism,
            user=self.metastore_conn_id.login,
            password=self.metastore_conn_id.password,
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
            asyncio.sleep(polling_interval)
        results = cursor.fetchall()
        if len(results) == 0:
            return "failure"
        return "success"
