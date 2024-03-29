import json
from unittest import mock

import pytest
from airflow import AirflowException
from airflow.models.connection import Connection
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook


class TestRedshiftDataHook:
    SQL_QUERY = "select * from any"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "query_ids, describe_statement_response, expected_result",
        [
            (["uuid"], {"Status": "FINISHED"}, {"status": "success", "completed_ids": ["uuid"]}),
            (
                ["uuid", "uuid"],
                {"Status": "FINISHED"},
                {"status": "success", "completed_ids": ["uuid", "uuid"]},
            ),
            (
                ["uuid"],
                {"Status": "FAILED", "QueryString": "select 1", "Error": "Test error"},
                {
                    "status": "error",
                    "message": "Error: select 1 query Failed due to, Test error",
                    "query_id": "uuid",
                    "type": "FAILED",
                },
            ),
            (
                ["uuid"],
                {"Status": "ABORTED"},
                {
                    "status": "error",
                    "message": "The query run was stopped by the user.",
                    "query_id": "uuid",
                    "type": "ABORTED",
                },
            ),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running")
    async def test_get_query_status(
        self, mock_is_still_running, mock_conn, query_ids, describe_statement_response, expected_result
    ):
        hook = RedshiftDataHook()
        mock_is_still_running.return_value = False
        mock_conn.return_value.describe_statement.return_value = describe_statement_response
        response = await hook.get_query_status(query_ids)
        assert response == expected_result

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running")
    async def test_get_query_status_with_sleep(self, mock_is_still_running, mock_conn):
        hook = RedshiftDataHook()
        mock_is_still_running.side_effect = [True, False]
        describe_statement_response = {"Status": "FINISHED"}
        mock_conn.return_value.describe_statement.return_value = describe_statement_response
        response = await hook.get_query_status(["uuid"])
        assert response == {"status": "success", "completed_ids": ["uuid"]}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.is_still_running")
    async def test_get_query_status_exception(self, mock_is_still_running, mock_conn):
        hook = RedshiftDataHook()
        mock_is_still_running.return_value = False
        mock_conn.return_value.describe_statement.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 500,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="redshift-data",
        )
        response = await hook.get_query_status(["uuid"])
        assert response == {
            "status": "error",
            "message": "An error occurred (SomeServiceException) when calling the "
            "redshift-data operation: Details/context around the exception or error",
            "type": "ERROR",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "query_id, describe_statement_response, expected_result",
        [
            ("uuid", {"Status": "PICKED"}, True),
            ("uuid", {"Status": "STARTED"}, True),
            ("uuid", {"Status": "SUBMITTED"}, True),
            ("uuid", {"Status": "FINISHED"}, False),
            ("uuid", {"Status": "FAILED"}, False),
            ("uuid", {"Status": "ABORTED"}, False),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    async def test_is_still_running(
        self, mock_client, query_id, describe_statement_response, expected_result
    ):
        hook = RedshiftDataHook()
        mock_client.return_value.describe_statement.return_value = describe_statement_response
        response = await hook.is_still_running(query_id)
        assert response == expected_result

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    async def test_is_still_running_exception(self, mock_conn):
        hook = RedshiftDataHook()
        mock_conn.return_value.describe_statement.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 500,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="redshift-data",
        )
        response = await hook.is_still_running("uuid")
        assert response == {
            "status": "error",
            "message": "An error occurred (SomeServiceException) when calling the "
            "redshift-data operation: Details/context around the exception or error",
            "type": "ERROR",
        }

    def test_execute_query_exception_exception_with_none_sql(self):
        hook = RedshiftDataHook(client_type="redshift-data")
        with pytest.raises(AirflowException):
            hook.execute_query(None, params=None)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    async def test_get_query_status_value_error(self, mock_client):
        hook = RedshiftDataHook()
        mock_client.side_effect = ValueError("Test value error")
        with pytest.raises(ValueError):
            await hook.get_query_status(["uuid"])

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    async def test_is_still_running_value_error(self, mock_client):
        hook = RedshiftDataHook()
        mock_client.side_effect = ValueError("Test value error")
        with pytest.raises(ValueError):
            await hook.is_still_running("uuid")

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    async def test_execute_query_value_error(self, mock_client):
        hook = RedshiftDataHook()
        mock_client.side_effect = ValueError("Test value error")
        with pytest.raises(ValueError):
            hook.execute_query("select * from table", params=None)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook.__init__")
    async def test_redshift_data_hook_value_error(self, mock_client):
        mock_client.side_effect = ValueError("Test value error")
        with pytest.raises(ValueError):
            RedshiftDataHook()

    @pytest.mark.parametrize(
        "sql,expected_response,expected_query_ids",
        [
            ("select * from table", {"Id": "uuid"}, ["uuid"]),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn_params")
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    def test_execute_query(self, mock_conn, mock_params, sql, expected_response, expected_query_ids):
        hook = RedshiftDataHook()
        mock_params.return_value = {
            "db_user": "test",
            "database": "test",
            "cluster_identifier": "test",
            "region_name": "test",
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
        }
        mock_conn.return_value.execute_statement.return_value = expected_response
        resp = hook.execute_query(sql, params=None)
        assert resp[0] == expected_query_ids

    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn_params")
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
    def test_execute_query_exception(self, mock_conn, mock_params):
        hook = RedshiftDataHook()
        mock_params.return_value = {
            "db_user": "test",
            "database": "test",
            "cluster_identifier": "test",
            "region_name": "test",
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
        }
        mock_conn.return_value.execute_statement.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
                "ResponseMetadata": {
                    "RequestId": "1234567890ABCDEF",
                    "HostId": "host ID data will appear here as a hash",
                    "HTTPStatusCode": 500,
                    "HTTPHeaders": {"header metadata key/values will appear here"},
                    "RetryAttempts": 0,
                },
            },
            operation_name="redshift-data",
        )

        resp = hook.execute_query(self.SQL_QUERY, params=None)
        assert resp == (
            [],
            {
                "status": "error",
                "message": "An error occurred (SomeServiceException) when calling the "
                "redshift-data operation: Details/context around the exception or error",
            },
        )

    @pytest.mark.parametrize(
        "connection_details, expected_output",
        [
            (
                {
                    "aws_access_key_id": "",
                    "aws_secret_access_key": "",
                    "db_user": "test_user",
                    "cluster_identifier": "",
                    "region_name": "",
                    "database": "",
                },
                {
                    "aws_access_key_id": "",
                    "aws_secret_access_key": "",
                    "db_user": "test_user",
                    "cluster_identifier": "",
                    "region_name": "",
                    "database": "",
                },
            ),
            (
                {
                    "access_key_id": "",
                    "secret_access_key": "",
                    "db_user": "test_user",
                    "cluster_identifier": "",
                    "region": "",
                    "database": "",
                },
                {
                    "aws_access_key_id": "",
                    "aws_secret_access_key": "",
                    "db_user": "test_user",
                    "cluster_identifier": "",
                    "region_name": "",
                    "database": "",
                },
            ),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_connection")
    def test_get_conn_params(self, mock_get_connection, connection_details, expected_output):
        mock_conn = Connection(extra=json.dumps(connection_details))
        mock_get_connection.return_value = mock_conn

        hook = RedshiftDataHook(client_type="redshift-data")
        response = hook.get_conn_params()
        assert response == expected_output

    @pytest.mark.parametrize(
        "connection_details, test",
        [
            (
                {
                    "access_key_id": "",
                    "secret_access_key": "",
                    "db_user": "test_user",
                    "cluster_identifier": "",
                    "region_name": "",
                },
                "",
            ),
            (
                {
                    "aws_access_key_id": "",
                    "aws_secret_access_key": "",
                    "cluster_identifier": "",
                    "region_name": "",
                    "database": "",
                },
                "",
            ),
            (
                {
                    "db_user": "test_user",
                    "cluster_identifier": "",
                    "region_name": "",
                    "database": "",
                },
                "",
            ),
            (
                {
                    "access_key_id": "",
                    "secret_access_key": "",
                    "db_user": "test_user",
                    "database": "",
                    "cluster_identifier": "",
                },
                "",
            ),
            (
                {
                    "access_key_id": "",
                    "secret_access_key": "",
                    "db_user": "test_user",
                    "region_name": "",
                    "database": "",
                },
                "",
            ),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_connection")
    def test_get_conn_params_exception(self, mock_get_connection, connection_details, test):
        response = json.dumps(connection_details)
        mock_conn = Connection(extra=response)
        mock_get_connection.return_value = mock_conn

        hook = RedshiftDataHook(client_type="redshift-data")
        with pytest.raises(AirflowException):
            hook.get_conn_params()

    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_connection")
    def test_get_conn_params_aws_conn_id_unset_exception(self, mock_get_connection):
        response = json.dumps(
            {
                "access_key_id": "",
                "secret_access_key": "",
                "db_user": "test_user",
                "cluster_identifier": "",
                "region_name": "",
                "database": "",
            }
        )
        mock_conn = Connection(extra=response)
        mock_get_connection.return_value = mock_conn

        hook = RedshiftDataHook(client_type="redshift-data")
        hook.aws_conn_id = None
        with pytest.raises(AirflowException):
            hook.get_conn_params()

    @pytest.mark.parametrize(
        "mock_login, mock_pwd, connection_details, expected_output",
        [
            (
                "test",
                "test",
                {
                    "db_user": "test_user",
                    "cluster_identifier": "test_cluster",
                    "region": "us-east-2",
                    "database": "test-redshift_database",
                    "aws_session_token": "test",
                },
                {
                    "aws_access_key_id": "test",
                    "aws_secret_access_key": "test",
                    "aws_session_token": "test",
                    "db_user": "test_user",
                    "cluster_identifier": "test_cluster",
                    "region_name": "us-east-2",
                    "database": "test-redshift_database",
                },
            ),
        ],
    )
    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_connection")
    def test_get_conn_params_with_login_pwd(
        self, mock_get_connection, mock_login, mock_pwd, connection_details, expected_output
    ):
        """
        Test get_conn_params by mocking the AWS secret and access key and session token,
        passing access and secret key in connection login and password instead passing in extra
        """
        mock_conn = Connection(login=mock_login, password=mock_pwd, extra=json.dumps(connection_details))
        mock_get_connection.return_value = mock_conn

        hook = RedshiftDataHook(client_type="redshift-data")
        response = hook.get_conn_params()
        assert response == expected_output
