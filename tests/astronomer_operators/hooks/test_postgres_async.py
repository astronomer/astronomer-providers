import asyncio

import pytest

from astronomer_operators.hooks.postgres import PostgresHookAsync


def test_rowcount():
    print("$$$$$$$$$$")
    print("Inside test_rowcount")
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(rowcount())
    loop.close()
    print("Result is")
    print(result)
    print("$$$$$$$$$$")
    assert result is None


@pytest.mark.asyncio
@pytest.mark.backend("postgres")
async def rowcount():
    print("###########")
    print("Inside test_rowcount_async")
    hook = PostgresHookAsync()
    print(hook.__dict__)
    response = await hook.run(
        sql="CREATE TABLE IF NOT EXISTS test_postgres_hook_table (c VARCHAR)"
    )
    print("###########")
    print("Response is..")
    print(response)
