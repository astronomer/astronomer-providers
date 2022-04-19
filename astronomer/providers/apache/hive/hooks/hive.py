from airflow.hooks.base import BaseHook
from impala.dbapi import connect
from impala.hiveserver2 import HiveServer2Connection


class HiveCliHookAsync(BaseHook):
    """
    Wrapper to interact with the Hive using impyla library

    :param conn_id: connection string for the hive
    """

    def __init__(self, conn_id: str) -> None:
        """Get the connection parameters separated from connection string"""
        self.conn = self.get_connection(conn_id=conn_id)

    def get_hive_client(self) -> HiveServer2Connection:
        """Makes a connection to the hive client using im library"""
        return connect(
            host=self.conn.host,
            port=self.conn.port,
            auth_mechanism="PLAIN",
            user=self.conn.login,
            password=self.conn.password,
        )
