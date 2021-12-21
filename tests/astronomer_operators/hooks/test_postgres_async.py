import asyncio
import os

import pytest

from astronomer_operators.hooks.postgres import PostgresHookAsync


def test_rowcount():
    os.environ[
        "AIRFLOW_CONN_POSTGRES_DEFAULT"
    ] = "postgresql://postgres:@localhost/airflow"
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(rowcount())
    loop.close()
    assert result["status"] == "success"
    assert result["message"] == "CREATE TABLE"


@pytest.mark.asyncio
async def rowcount():
    hook = PostgresHookAsync()
    print(hook.__dict__)
    response = await hook.run(
        sql="CREATE TABLE IF NOT EXISTS test_postgres_hook_table (c VARCHAR)"
    )
    return response
