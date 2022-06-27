from unittest import mock

import pytest
from botocore.exceptions import ClientError

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


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.get_conn")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.is_still_running")
async def test_get_query_status_exception(mock_is_still_running, mock_conn):
    hook = RedshiftSQLHookAsync()
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
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.get_conn")
async def test_is_still_running_exception(mock_conn):
    hook = RedshiftSQLHookAsync()
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
