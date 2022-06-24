from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger

TEST_CONN_ID = "aws_default"
TEST_TASK_ID = "123"
POLL_INTERVAL = 4.0


def test_redshift_data_trigger_serialization():
    """
    Asserts that the RedshiftDataTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = RedshiftDataTrigger(
        task_id=TEST_TASK_ID,
        poll_interval=POLL_INTERVAL,
        aws_conn_id=TEST_CONN_ID,
        query_ids=[],
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.redshift_data.RedshiftDataTrigger"
    assert kwargs == {
        "task_id": TEST_TASK_ID,
        "poll_interval": POLL_INTERVAL,
        "aws_conn_id": TEST_CONN_ID,
        "query_ids": [],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_ids,return_value,response",
    [
        (
            ["uuid", "uuid"],
            {"status": "error", "message": "test error"},
            TriggerEvent({"status": "error", "message": "test error"}),
        ),
        (["uuid", "uuid"], False, TriggerEvent({"status": "error", "message": f"{TEST_TASK_ID} failed"})),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_query_status")
async def test_redshift_data_trigger_run(mock_get_query_status, query_ids, return_value, response):
    """
    Tests that RedshiftDataTrigger only fires once the query execution reaches a successful state.
    """
    mock_get_query_status.return_value = return_value
    trigger = RedshiftDataTrigger(
        task_id=TEST_TASK_ID,
        poll_interval=POLL_INTERVAL,
        aws_conn_id=TEST_CONN_ID,
        query_ids=query_ids,
    )
    generator = trigger.run()
    actual = await generator.asend(None)
    assert response == actual


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.get_query_status")
async def test_redshift_data_trigger_exception(mock_get_query_status, query_ids):
    """
    Test that RedshiftDataTrigger fires the correct event in case of an error.
    """
    mock_get_query_status.side_effect = Exception("Test exception")

    trigger = RedshiftDataTrigger(
        task_id=TEST_TASK_ID,
        poll_interval=POLL_INTERVAL,
        aws_conn_id=TEST_CONN_ID,
        query_ids=query_ids,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
