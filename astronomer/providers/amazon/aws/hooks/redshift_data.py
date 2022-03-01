from io import StringIO
from typing import Dict, Iterable, List, Optional, Union

import botocore.exceptions
from airflow import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from snowflake.connector.util_text import split_statements


class RedshiftDataHook(AwsBaseHook):
    def __init__(self, *args, **kwargs) -> None:
        client_type: str = "redshift-data"
        kwargs["client_type"] = "redshift-data"
        kwargs["resource_type"] = "redshift-data"
        super().__init__(*args, **kwargs)
        self.client_type = client_type

    def get_conn_params(self) -> Dict[str, Union[str, int]]:
        """Helper method to retrieve connection args"""
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
        else:
            raise AirflowException("Required access_key_id, aws_secret_access_key")

        if "region" in extra_config or "region_name" in extra_config:
            self.log.info("Retrieving region_name from Connection.extra_config['region_name']")
            conn_params["region_name"] = (
                extra_config["region"] if "region" in extra_config else extra_config["region_name"]
            )
        else:
            raise AirflowException("Required Region name is missing !")

        if "cluster_identifier" in extra_config:
            self.log.info("Retrieving cluster_identifier from Connection.extra_config['cluster_identifier']")
            conn_params["cluster_identifier"] = extra_config["cluster_identifier"]
        else:
            raise AirflowException("Required Cluster identifier is missing !")

        return conn_params

    def execute_query(self, sql: Union[str, Iterable[str]], params: Optional[Dict]):
        """
        Runs an SQL statement, which can be data manipulation language (DML)
        or data definition language (DDL)

        :param sql: list of query ids
        """
        try:
            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            client = self.get_conn()
            conn_params = self.get_conn_params()
            query_ids: List[str] = []
            for sql_statement in sql:
                self.log.info(f"Executing statement: {sql_statement}")
                response = client.execute_statement(
                    Database=conn_params["database"],
                    ClusterIdentifier=conn_params["cluster_identifier"],
                    DbUser=conn_params["db_user"],
                    Sql=sql_statement,
                    WithEvent=True,
                )
                query_ids.append(response["Id"])
            return query_ids
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error)}
