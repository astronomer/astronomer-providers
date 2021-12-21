import asyncio
import os

import pytest

from astronomer_operators.postgres.hooks.postgres import PostgresHookAsync


def test_rowcount():
    os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = "postgresql://postgres:@localhost/airflow"
    loop = asyncio.get_event_loop()
    input_data = ["foo", "bar", "baz"]
    result_create_response, result_insert_query_response = loop.run_until_complete(rowcount(input_data))
    loop.close()
    assert result_create_response["status"] == "success"
    assert result_insert_query_response["status"] == "success"
    assert result_create_response["message"] == "CREATE TABLE"
    assert result_insert_query_response["message"] == "INSERT 0 3"
    assert int(result_insert_query_response["message"].split(" ")[2]) == len(input_data)


@pytest.mark.asyncio
async def rowcount(input_data):
    hook = PostgresHookAsync()
    values = ",".join(f"('{data}')" for data in input_data)
    create_response = await hook.run(sql="CREATE TABLE IF NOT EXISTS test_postgres_hook_table (c VARCHAR)")
    insert_query_response = await hook.run(sql=f"INSERT INTO test_postgres_hook_table VALUES {values}")
    return create_response, insert_query_response
