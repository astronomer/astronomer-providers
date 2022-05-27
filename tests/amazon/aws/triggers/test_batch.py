import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.batch import BatchOperatorTrigger

JOB_NAME = "51455483-c62c-48ac-9b88-53a6a725baa3"
JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
MAX_RETRIES = 2
STATUS_RETRIES = 3


def test_batch_trigger_serialization():
    """
    Asserts that the BatchOperatorTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.batch.BatchOperatorTrigger"
    assert kwargs == {
        "job_id": JOB_ID,
        "job_name": JOB_NAME,
        "job_definition": "hello-world",
        "job_queue": "queue",
        "waiters": None,
        "tags": {},
        "max_retries": MAX_RETRIES,
        "status_retries": STATUS_RETRIES,
        "parameters": {},
        "overrides": {},
        "array_properties": {},
        "region_name": "eu-west-1",
        "aws_conn_id": "airflow_test",
    }


@pytest.mark.asyncio
async def test_batch_trigger_run():
    """Test that the task is not done when event is not returned from trigger."""
    trigger = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)
    # TriggerEvent was not returned
    assert task.done() is False


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
async def test_batch_trigger_completed(mock_response):
    """Test if the success event is  returned from trigger."""
    mock_response.return_value = {"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"}
    trigger = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )
    generator = trigger.run()
    actual_response = await generator.asend(None)
    assert (
        TriggerEvent({"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"})
        == actual_response
    )


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
async def test_batch_trigger_failure(mock_response):
    """Test if the failure event is returned from trigger."""
    mock_response.return_value = {"status": "error", "message": f"{JOB_ID} failed"}
    trigger = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )
    generator = trigger.run()
    actual_response = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": f"{JOB_ID} failed"}) == actual_response


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
async def test_batch_trigger_none(mock_response):
    """Test if the failure event is returned when there is no response from hook."""
    mock_response.return_value = None
    trigger = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )
    generator = trigger.run()
    actual_response = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": f"{JOB_ID} failed"}) == actual_response


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
async def test_batch_trigger_exception(mock_response):
    """Test if the exception is raised from trigger."""
    mock_response.side_effect = Exception("Test exception")
    trigger = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
