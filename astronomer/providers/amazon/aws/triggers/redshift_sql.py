import asyncio
from ctypes import Union
from typing import Any, Dict, Iterable, Tuple

from airflow import Optional
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHookAsync


class RedshiftSQLTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        redshift_conn_id: str,
        cluster_identifier: str,
        sql: Optional[Union[Dict, Iterable]],
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.redshift_conn_id = redshift_conn_id
        self.cluster_identifier = cluster_identifier
        self.sql = sql

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes RedshiftSQLTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.amazon.aws.triggers.redshift_sql.RedshiftSQLTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "redshift_conn_id": self.redshift_conn_id,
                "cluster_identifier": self.cluster_identifier,
                "sql": self.sql,
            },
        )

    async def run(self):
        """
        Make async connection to redshiftSQL and execute query using
        the Amazon Redshift Data API to interact with Amazon Redshift clusters
        """
        hook = RedshiftSQLHookAsync(redshift_conn_id=self.redshift_conn_id)
        while True:
            try:
                response = await hook.execute_query(cluster_identifier=self.cluster_identifier)
                if response:
                    yield TriggerEvent(response)
                    return
                else:
                    error_message = f"{self.task_id} failed"
                    raise AirflowException(error_message)
            except AirflowException:
                await asyncio.sleep(self.polling_period_seconds)
