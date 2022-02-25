import json
from unittest import mock

import pytest
from airflow.models.connection import Connection

from astronomer.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from astronomer.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHookAsync


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_ids, describe_statement_response, expected_result",
    [
        (["uuid"], {"Status": "FINISHED"}, {"status": "success", "completed_ids": ["uuid"]}),
        (["uuid", "uuid"], {"Status": "FINISHED"}, {"status": "success", "completed_ids": ["uuid", "uuid"]}),
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
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.get_conn")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.is_still_running")
async def test_get_query_status(
    mock_is_still_running, mock_conn, query_ids, describe_statement_response, expected_result
):
    hook = RedshiftSQLHookAsync()
    mock_is_still_running.return_value = False
    mock_conn.return_value.describe_statement.return_value = describe_statement_response
    response = await hook.get_query_status(query_ids)
    assert response == expected_result


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
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.get_conn")
async def test_is_still_running(mock_client, query_id, describe_statement_response, expected_result):
    hook = RedshiftSQLHookAsync()
    mock_client.return_value.describe_statement.return_value = describe_statement_response
    response = await hook.is_still_running(query_id)
    assert response == expected_result


@pytest.mark.parametrize(
    "sql,expected_response,expected_query_ids",
    [
        ("select * from table", {"Id": "uuid"}, ["uuid"]),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn_params")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_conn")
def test_execute_query(mock_conn, mock_params, sql, expected_response, expected_query_ids):
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
    assert resp == expected_query_ids


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
def test_get_conn_params(mock_get_connection, connection_details, expected_output):
    mock_conn = Connection(extra=json.dumps(connection_details))
    mock_get_connection.return_value = mock_conn

    hook = RedshiftDataHook(client_type="redshift-data")
    response = hook.get_conn_params()
    assert response == expected_output
