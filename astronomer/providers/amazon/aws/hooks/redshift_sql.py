import logging
from typing import Dict, Iterable, List, Optional, Union

import botocore.exceptions
from aiobotocore.session import get_session
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from asgiref.sync import sync_to_async

log = logging.getLogger(__name__)


class RedshiftSQLHookAsync(RedshiftSQLHook):
    """
    Interact with AWS Redshift using aiobotocore python library
    """

    def __init__(self, *args, **kwargs) -> None:
        client_type: str = "redshift-data"
        kwargs["resource_type"] = "redshift-data"
        super().__init__(*args, **kwargs)
        self.client_type = client_type

    async def get_redshift_connection_params(self):
        connection_object = await sync_to_async(self.get_connection)(self.aws_conn_id)
        extra_config = connection_object.extra_dejson

        conn_params: Dict[str, Union[str, int]] = {}

        if connection_object.login:
            conn_params["user"] = connection_object.login
        if connection_object.password:
            conn_params["password"] = connection_object.password
        if connection_object.host:
            conn_params["host"] = connection_object.host
        if connection_object.port:
            conn_params["port"] = connection_object.port
        if connection_object.schema:
            conn_params["database"] = connection_object.schema

        if "aws_access_key_id" in extra_config and "aws_secret_access_key" in extra_config:
            conn_params["aws_access_key_id"] = extra_config["aws_access_key_id"]
            conn_params["aws_secret_access_key"] = extra_config["aws_secret_access_key"]
            conn_params["aws_session_token"] = extra_config.get("aws_session_token")

        if "region_name" in extra_config:
            self.log.info("Retrieving region_name from Connection.extra_config['region_name']")
            conn_params["region_name"] = extra_config["region_name"]

        return conn_params

    async def get_redshift_client_async(self, conn_params):
        async_client_session = get_session()
        return async_client_session.create_client(
            service_name=self.client_type,
            region_name=conn_params["region_name"],
            aws_secret_access_key=conn_params["aws_secret_access_key"],
            aws_access_key_id=conn_params["aws_access_key_id"],
            aws_session_token=conn_params["aws_session_token"],
        )

    async def execute_query(
        self, cluster_identifier: str, sql: Optional[Union[Dict, Iterable]], database: str, user: str
    ):
        """
        Connects to the AWS redshift  via aiobotocore and execute the
        """
        connection_params = await self.get_redshift_connection_params()
        query_ids: List[str] = []
        async with await self.get_redshift_client_async(connection_params) as client:
            try:
                self.log.info(f"Executing statement: {self.sql}")
                response = await client.execute_statement(
                    Database=database,
                    ClusterIdentifier=cluster_identifier,
                    DbUser=user,
                    Sql=sql,
                    WithEvent=True,
                )
                query_ids.append(response["Id"])
                return query_ids
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error)}

    async def get_query_status(self, query_ids: List[str]):
        """
        Async function to get the Query status by query Ids, this function takes list of query_ids make
        sync_to_async connection
        to snowflake to get the query status by query id returns the query status.
        """
        connection_params = await self.get_redshift_connection_params()
        async with await self.get_redshift_client_async(connection_params) as client:
            try:
                for id in query_ids:
                    desc = client.describe_statement(Id=id)
                    if desc["Status"] == "FINISHED":
                        print(desc["ResultRows"])
                        return {"status": "success", "message": "Finished executing the Query"}
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error)}
