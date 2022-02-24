import logging
from io import StringIO
from typing import Dict, Iterable, List, Optional, Union

import botocore.exceptions
from aiobotocore.session import get_session
from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from asgiref.sync import sync_to_async
from async_timeout import asyncio
from snowflake.connector.util_text import split_statements

log = logging.getLogger(__name__)


class RedshiftSQLHookAsync(RedshiftSQLHook):
    """
    Interact with AWS Redshift using aiobotocore python library, inherite RedshiftSQLHook class
    """

    def __init__(self, *args, **kwargs) -> None:
        client_type: str = "redshift-data"
        kwargs["resource_type"] = "redshift-data"
        super().__init__(*args, **kwargs)
        self.client_type = client_type

    async def get_redshift_connection_params(self):
        connection_object = await sync_to_async(self.get_connection)(self.redshift_conn_id)
        extra_config = connection_object.extra_dejson

        conn_params: Dict[str, Union[str, int]] = {}

        if "db_user" in extra_config:
            conn_params["db_user"] = extra_config.get("db_user", None)

        if "database" in extra_config:
            conn_params["database"] = extra_config.get("database", None)
        elif connection_object.schema:
            conn_params["database"] = connection_object.schema
        else:
            raise AirflowException("Required Database name ")

        if "access_key_id" in extra_config or "aws_access_key_id" in extra_config:
            conn_params["aws_access_key_id"] = (
                extra_config["access_key_id"]
                if "access_key_id" in extra_config
                else extra_config["aws_access_key_id"]
            )
            conn_params["aws_secret_access_key"] = (
                extra_config["secret_access_key"]
                if "secret_access_key" in extra_config
                else extra_config["aws_secret_access_key"]
            )

        if "region" in extra_config or "region_name" in extra_config:
            self.log.info("Retrieving region_name from Connection.extra_config['region_name']")
            conn_params["region_name"] = (
                extra_config["region"] if "region" in extra_config else extra_config["region_name"]
            )
        else:
            raise AirflowException("Required Region name")

        if "cluster_identifier" in extra_config:
            self.log.info("Retrieving cluster_identifier from Connection.extra_config['cluster_identifier']")
            conn_params["cluster_identifier"] = extra_config["cluster_identifier"]
        else:
            raise AirflowException("Required Cluster identifier")

        return conn_params

    async def get_redshift_client_async(self, conn_params):
        """
        Gets the async aiobotocore session with
        aws_secret_access_key, aws_access_key_id, region_name
        and returns  async create client

        :param conn_params: connection parameter dict object
        """
        async_client_session = get_session()
        return async_client_session.create_client(
            service_name=self.client_type,
            region_name=conn_params["region_name"],
            aws_secret_access_key=conn_params["aws_secret_access_key"],
            aws_access_key_id=conn_params["aws_access_key_id"],
        )

    async def execute_query(self, sql: Optional[Union[Dict, Iterable]], params: Optional[Dict]):
        """
        Connects to the AWS redshift via aiobotocore, running a query is asynchronous;
        running a statement returns an ExecuteStatementOutput, which includes the statement ID.

        :param sql: list of sql queries
        :param params: Query parameters
        """
        if isinstance(sql, str):
            split_statements_tuple = split_statements(StringIO(sql))
            sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

        connection_params = await self.get_redshift_connection_params()
        query_ids: List[str] = []
        async with await self.get_redshift_client_async(connection_params) as client:
            try:
                for sql_statement in sql:
                    self.log.info(f"Executing statement: {sql_statement}")
                    if params:
                        response = await client.execute_statement(
                            Database=connection_params["database"],
                            ClusterIdentifier=connection_params["cluster_identifier"],
                            DbUser=connection_params["db_user"],
                            Sql=sql_statement,
                            Parameters=params,
                            WithEvent=True,
                        )
                    else:
                        response = await client.execute_statement(
                            Database=connection_params["database"],
                            ClusterIdentifier=connection_params["cluster_identifier"],
                            DbUser=connection_params["db_user"],
                            Sql=sql_statement,
                            WithEvent=True,
                        )
                    query_ids.append(response["Id"])
                # res = await self.get_query_status(query_ids=query_ids)
                return query_ids
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error)}

    async def get_query_status(self, query_ids: List[str]):
        """
        Async function to get the Query status by query Ids, this function
        takes list of query_ids make async connection
        to redshift data to get the query status by query id returns the query status.

        :param sql: list of query ids
        """
        connection_params = await self.get_redshift_connection_params()
        async with await self.get_redshift_client_async(connection_params) as client:
            try:
                completed_ids: List[str] = []
                for id in query_ids:
                    while await self.is_still_running(id):
                        await asyncio.sleep(1)
                    res = await client.describe_statement(Id=id)
                    if res["Status"] == "FINISHED":
                        completed_ids.append(id)
                    elif res["Status"] == "FAILED":
                        msg = "Error: " + res["QueryString"] + " query Failed due to, " + res["Error"]
                        return {"status": "error", "message": msg, "query_id": id, "type": res["Status"]}
                    elif res["Status"] == "ABORTED":
                        return {
                            "status": "error",
                            "message": "The query run was stopped by the user.",
                            "query_id": id,
                            "type": res["Status"],
                        }
                return {"status": "success", "completed_ids": completed_ids}
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error), "type": "ERROR"}

    async def is_still_running(self, id: str):
        """
        Async function to whether the query is still running or in
        "PICKED", "STARTED", "SUBMITTED" state and returns True else
        return False
        """
        connection_params = await self.get_redshift_connection_params()
        async with await self.get_redshift_client_async(connection_params) as client:
            try:
                desc = await client.describe_statement(Id=id)
                if desc["Status"] in ["PICKED", "STARTED", "SUBMITTED"]:
                    return True
                return False
            except botocore.exceptions.ClientError as error:
                return {"status": "error", "message": str(error), "type": "ERROR"}
