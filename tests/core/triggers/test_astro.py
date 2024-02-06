import asyncio
from unittest.mock import patch

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.core.triggers.astro import AstroDeploymentTrigger


class TestAstroDeploymentTrigger:

    def test_serialize(self):
        trigger = AstroDeploymentTrigger(
            external_dag_id="external_dag_id",
            dag_run_id="dag_run_id",
            external_task_id="external_task_id",
            astro_cloud_conn_id="astro_cloud_conn_id",
            poke_interval=1.0,
        )

        serialized_data = trigger.serialize()

        expected_result = (
            "astronomer.providers.core.triggers.astro.AstroDeploymentTrigger",
            {
                "external_dag_id": "external_dag_id",
                "external_task_id": "external_task_id",
                "dag_run_id": "dag_run_id",
                "astro_cloud_conn_id": "astro_cloud_conn_id",
                "poke_interval": 1.0,
            },
        )

        assert serialized_data == expected_result

    @pytest.mark.asyncio
    @patch("astronomer.providers.core.hooks.astro.AstroHook.get_task_instance")
    async def test_run_task_successful(self, mock_get_task_instance):
        trigger = AstroDeploymentTrigger(
            external_dag_id="external_dag_id",
            dag_run_id="dag_run_id",
            external_task_id="external_task_id",
            astro_cloud_conn_id="astro_cloud_conn_id",
            poke_interval=1.0,
        )

        mock_get_task_instance.return_value = {"state": "success"}

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "done"})

    @pytest.mark.asyncio
    @patch("astronomer.providers.core.hooks.astro.AstroHook.get_task_instance")
    async def test_run_task_failed(self, mock_get_task_instance):
        trigger = AstroDeploymentTrigger(
            external_dag_id="external_dag_id",
            dag_run_id="dag_run_id",
            external_task_id="external_task_id",
            astro_cloud_conn_id="astro_cloud_conn_id",
            poke_interval=1.0,
        )

        mock_get_task_instance.return_value = {"state": "failed"}

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "failed"})

    @pytest.mark.asyncio
    @patch("astronomer.providers.core.hooks.astro.AstroHook.get_dag_run")
    async def test_run_dag_successful(self, mock_get_dag_run):
        trigger = AstroDeploymentTrigger(
            external_dag_id="external_dag_id",
            dag_run_id="dag_run_id",
            astro_cloud_conn_id="astro_cloud_conn_id",
            poke_interval=1.0,
        )

        # Mocking AstroHook responses for a successful DAG run
        mock_get_dag_run.return_value = {"state": "success"}

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "done"})

    @pytest.mark.asyncio
    @patch("astronomer.providers.core.hooks.astro.AstroHook.get_dag_run")
    async def test_run_dag_failed(self, mock_get_dag_run):
        trigger = AstroDeploymentTrigger(
            external_dag_id="external_dag_id",
            dag_run_id="dag_run_id",
            astro_cloud_conn_id="astro_cloud_conn_id",
            poke_interval=1.0,
        )

        mock_get_dag_run.return_value = {"state": "failed"}

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "failed"})

    @pytest.mark.asyncio
    @patch("astronomer.providers.core.hooks.astro.AstroHook.get_dag_run")
    async def test_run_dag_wait(self, mock_get_dag_run):
        trigger = AstroDeploymentTrigger(
            external_dag_id="external_dag_id",
            dag_run_id="dag_run_id",
            astro_cloud_conn_id="astro_cloud_conn_id",
            poke_interval=1.0,
        )

        mock_get_dag_run.return_value = {"state": "running"}

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()
