import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.batch import (
    BatchOperatorTrigger,
    BatchSensorTrigger,
)

JOB_NAME = "51455483-c62c-48ac-9b88-53a6a725baa3"
JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
MAX_RETRIES = 2
STATUS_RETRIES = 3
POKE_INTERVAL = 5
AWS_CONN_ID = "airflow_test"
REGION_NAME = "eu-west-1"


class TestBatchOperatorTrigger:
    TRIGGER = BatchOperatorTrigger(
        job_id=JOB_ID,
        job_name=JOB_NAME,
        job_definition="hello-world",
        job_queue="queue",
        waiters=None,
        tags={},
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters={},
        container_overrides={},
        array_properties={},
        region_name="eu-west-1",
        aws_conn_id="airflow_test",
    )

    def test_batch_trigger_serialization(self):
        """
        Asserts that the BatchOperatorTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
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
            "container_overrides": {},
            "array_properties": {},
            "region_name": "eu-west-1",
            "aws_conn_id": "airflow_test",
        }

    @pytest.mark.asyncio
    async def test_batch_trigger_run(self):
        """Test that the task is not done when event is not returned from trigger."""

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)
        # TriggerEvent was not returned
        assert task.done() is False

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
    async def test_batch_trigger_completed(self, mock_response):
        """Test if the success event is  returned from trigger."""
        mock_response.return_value = {"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"}

        generator = self.TRIGGER.run()
        actual_response = await generator.asend(None)
        assert (
            TriggerEvent({"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"})
            == actual_response
        )

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
    async def test_batch_trigger_failure(self, mock_response):
        """Test if the failure event is returned from trigger."""
        mock_response.return_value = {"status": "error", "message": f"{JOB_ID} failed"}

        generator = self.TRIGGER.run()
        actual_response = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": f"{JOB_ID} failed"}) == actual_response

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
    async def test_batch_trigger_none(self, mock_response):
        """Test if the failure event is returned when there is no response from hook."""
        mock_response.return_value = None

        generator = self.TRIGGER.run()
        actual_response = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": f"{JOB_ID} failed"}) == actual_response

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.monitor_job")
    async def test_batch_trigger_exception(self, mock_response):
        """Test if the exception is raised from trigger."""
        mock_response.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


class TestBatchSensorTrigger:
    TRIGGER = BatchSensorTrigger(
        job_id=JOB_ID,
        region_name=REGION_NAME,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=POKE_INTERVAL,
    )

    def test_batch_sensor_trigger_serialization(self):
        """
        Asserts that the BatchSensorTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == "astronomer.providers.amazon.aws.triggers.batch.BatchSensorTrigger"
        assert kwargs == {
            "job_id": JOB_ID,
            "region_name": "eu-west-1",
            "aws_conn_id": "airflow_test",
            "poke_interval": POKE_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.get_job_description")
    async def test_batch_sensor_trigger_run(self, mock_response):
        """Trigger the BatchSensorTrigger and check if the task is in running state."""
        mock_response.return_value = {"status": "RUNNABLE"}

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)
        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.get_job_description")
    async def test_batch_sensor_trigger_completed(self, mock_response):
        """Test if the success event is returned from trigger."""
        mock_response.return_value = {"status": "SUCCEEDED"}
        trigger = BatchSensorTrigger(
            job_id=JOB_ID,
            region_name=REGION_NAME,
            aws_conn_id=AWS_CONN_ID,
        )
        generator = trigger.run()
        actual_response = await generator.asend(None)
        assert (
            TriggerEvent({"status": "success", "message": f"{JOB_ID} was completed successfully"})
            == actual_response
        )

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.get_job_description")
    async def test_batch_sensor_trigger_failure(self, mock_response):
        """Test if the failure event is returned from trigger."""
        mock_response.return_value = {"status": "FAILED"}
        trigger = BatchSensorTrigger(
            job_id=JOB_ID,
            region_name=REGION_NAME,
            aws_conn_id=AWS_CONN_ID,
        )
        generator = trigger.run()
        actual_response = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": f"{JOB_ID} failed"}) == actual_response

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.batch_client.BatchClientHookAsync.get_job_description")
    async def test_batch_sensor_trigger_exception(self, mock_response):
        """Test if the exception is raised from trigger."""
        mock_response.side_effect = Exception("Test exception")
        trigger = BatchSensorTrigger(
            job_id=JOB_ID,
            region_name=REGION_NAME,
            aws_conn_id=AWS_CONN_ID,
        )
        task = [i async for i in trigger.run()]
        assert len(task) == 1

        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
