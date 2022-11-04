import asyncio
from io import StringIO
from typing import Any, Dict, Iterable, List, Tuple, Union

import botocore.exceptions
from airflow.exceptions import AirflowException
from airflow.models.param import ParamsDict
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from asgiref.sync import sync_to_async
from snowflake.connector.util_text import split_statements


class RedshiftDataHook(AwsBaseHook):
    """
    RedshiftDataHook inherits from AwsBaseHook to connect with AWS redshift
    by using boto3 client_type as redshift-data we can interact with redshift cluster database and execute the query

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param verify: Whether or not to verify SSL certificates.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param client_type: boto3.client client_type. Eg 's3', 'emr' etc
    :param resource_type: boto3.resource resource_type. Eg 'dynamodb' etc
    :param config: Configuration for botocore client.
        (https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(self, *args: Any, poll_interval: int = 0, **kwargs: Any) -> None:
        aws_connection_type: str = "redshift-data"
        try:
            # for apache-airflow-providers-amazon>=3.0.0
            kwargs["client_type"] = aws_connection_type
            kwargs["resource_type"] = aws_connection_type
            super().__init__(*args, **kwargs)
        except ValueError:
            # for apache-airflow-providers-amazon>=4.1.0
            kwargs["client_type"] = aws_connection_type
            super().__init__(*args, **kwargs)
        self.client_type = aws_connection_type
        self.poll_interval = poll_interval

    def get_conn_params(self) -> Dict[str, Union[str, int]]:
        """Helper method to retrieve connection args"""
        if not self.aws_conn_id:
            raise AirflowException("Required connection details is missing !")

        connection_object = self.get_connection(self.aws_conn_id)
        extra_config = connection_object.extra_dejson

        conn_params: Dict[str, Union[str, int]] = {}

        if "db_user" in extra_config:
            conn_params["db_user"] = extra_config.get("db_user", None)
        else:
            raise AirflowException("Required db user is missing !")

        if "database" in extra_config:
            conn_params["database"] = extra_config.get("database", None)
        elif connection_object.schema:
            conn_params["database"] = connection_object.schema
        else:
            raise AirflowException("Required Database name is missing !")

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
        elif connection_object.login:
            conn_params["aws_access_key_id"] = connection_object.login
            conn_params["aws_secret_access_key"] = connection_object.password
        else:
            raise AirflowException("Required access_key_id, aws_secret_access_key")

        if "region" in extra_config or "region_name" in extra_config:
            self.log.info("Retrieving region_name from Connection.extra_config['region_name']")
            conn_params["region_name"] = (
                extra_config["region"] if "region" in extra_config else extra_config["region_name"]
            )
        else:
            raise AirflowException("Required Region name is missing !")

        if "aws_session_token" in extra_config:
            self.log.info(
                "session token retrieved from extra, please note you are responsible for renewing these.",
            )
            conn_params["aws_session_token"] = extra_config["aws_session_token"]

        if "cluster_identifier" in extra_config:
            self.log.info("Retrieving cluster_identifier from Connection.extra_config['cluster_identifier']")
            conn_params["cluster_identifier"] = extra_config["cluster_identifier"]
        else:
            raise AirflowException("Required Cluster identifier is missing !")

        return conn_params

    def execute_query(
        self, sql: Union[Dict[Any, Any], Iterable[Any]], params: Union[ParamsDict, Dict[Any, Any]]
    ) -> Tuple[List[str], Dict[str, str]]:
        """
        Runs an SQL statement, which can be data manipulation language (DML)
        or data definition language (DDL)

        :param sql: list of query ids
        """
        if not sql:
            raise AirflowException("SQL query is None.")
        try:
            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = self.get_conn()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = self.get_conn()
            conn_params = self.get_conn_params()
            query_ids: List[str] = []
            for sql_statement in sql:
                self.log.info("Executing statement: %s", sql_statement)
                response = client.execute_statement(
                    Database=conn_params["database"],
                    ClusterIdentifier=conn_params["cluster_identifier"],
                    DbUser=conn_params["db_user"],
                    Sql=sql_statement,
                    WithEvent=True,
                )
                query_ids.append(response["Id"])
            return query_ids, {"status": "success", "message": "success"}
        except botocore.exceptions.ClientError as error:
            return [], {"status": "error", "message": str(error)}

    async def get_query_status(self, query_ids: List[str]) -> Dict[str, Union[str, List[str]]]:
        """
        Async function to get the Query status by query Ids.
        The function takes list of query_ids, makes async connection to redshift data to get the query status
        by query id and returns the query status. In case of success, it returns a list of query IDs of the queries
        that have a status `FINISHED`. In the case of partial failure meaning if any of queries fail or is aborted by
        the user we return an error as a whole.

        :param query_ids: list of query ids
        """
        try:
            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = await sync_to_async(self.get_conn)()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = await sync_to_async(self.get_conn)()
            completed_ids: List[str] = []
            for query_id in query_ids:
                while await self.is_still_running(query_id):
                    await asyncio.sleep(self.poll_interval)
                res = client.describe_statement(Id=query_id)
                if res["Status"] == "FINISHED":
                    completed_ids.append(query_id)
                elif res["Status"] == "FAILED":
                    msg = "Error: " + res["QueryString"] + " query Failed due to, " + res["Error"]
                    return {"status": "error", "message": msg, "query_id": query_id, "type": res["Status"]}
                elif res["Status"] == "ABORTED":
                    return {
                        "status": "error",
                        "message": "The query run was stopped by the user.",
                        "query_id": query_id,
                        "type": res["Status"],
                    }
            return {"status": "success", "completed_ids": completed_ids}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}

    async def is_still_running(self, qid: str) -> Union[bool, Dict[str, str]]:
        """
        Async function to check whether the query is still running to return True or in
        "PICKED", "STARTED" or "SUBMITTED" state to return False.
        """
        try:
            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = await sync_to_async(self.get_conn)()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = await sync_to_async(self.get_conn)()
            desc = client.describe_statement(Id=qid)
            if desc["Status"] in ["PICKED", "STARTED", "SUBMITTED"]:
                return True
            return False
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}
