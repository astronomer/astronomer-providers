import asyncio
from typing import Dict, Union

import pytest

from astronomer_operators.snowflake.triggers.snowflake_trigger import SnowflakeTrigger
from tests.astronomer_operators.snowflake.operators.test_snowflake_async import (
    CONN_ID,
    POLLING_PERIOD_SECONDS,
    TASK_ID,
)

BASE_CONNECTION_KWARGS: Dict[str, Union[str, Dict[str, str]]] = {
    "login": "user",
    "password": "pw",
    "schema": "public",
    "extra": {
        "database": "db",
        "account": "airflow",
        "warehouse": "af_wh",
        "region": "af_region",
        "role": "af_role",
    },
}


class TestSnowflakeTriggerAsync:
    @pytest.mark.parametrize(
        "query_ids",
        [
            (["uuid", "uuid"]),
            (["uuid", "uuid", "uuid2", "uuid2"]),
            (["uuid", "uuid"]),
            (["uuid", "uuid", "uuid2", "uuid2"]),
        ],
    )
    def test_run_storing_query_ids_extra(self, query_ids):
        """
        Asserts that the SnowflakeTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = SnowflakeTrigger(
            task_id=TASK_ID,
            polling_period_seconds=POLLING_PERIOD_SECONDS,
            query_ids=query_ids,
            snowflake_conn_id=CONN_ID,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer_operators.snowflake.triggers.snowflake_trigger.SnowflakeTrigger"
        assert kwargs == {
            "task_id": TASK_ID,
            "polling_period_seconds": 1.0,
            "query_ids": query_ids[::2],
            "snowflake_conn_id": CONN_ID,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "query_ids",
        [
            (["uuid", "uuid"]),
            (["uuid", "uuid", "uuid2", "uuid2"]),
            (["uuid", "uuid"]),
            (["uuid", "uuid", "uuid2", "uuid2"]),
        ],
    )
    async def test_databricks_trigger_success(query_ids):
        """
        Tests that the SnowflakeTrigger only fires once a
        snowflake run.
        """
        trigger = SnowflakeTrigger(
            task_id=TASK_ID,
            polling_period_seconds=POLLING_PERIOD_SECONDS,
            query_ids=query_ids,
            snowflake_conn_id=CONN_ID,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()
