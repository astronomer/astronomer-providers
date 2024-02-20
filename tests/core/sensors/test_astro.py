from unittest import mock

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException, TaskDeferred

from astronomer.providers.core.sensors.astro import ExternalDeploymentSensor
from astronomer.providers.core.triggers.astro import AstroDeploymentTrigger


class TestExternalDeploymentSensor:

    @pytest.mark.parametrize(
        "get_dag_runs_response",
        [
            None,
            [],
            [{"dag_run_id": "run_id", "state": "success"}],
            [{"dag_run_id": "run_id", "state": "running"}],
            [{"dag_run_id": "run_id", "state": "queued"}],
        ],
    )
    @mock.patch("astronomer.providers.core.hooks.astro.AstroHook.get_dag_runs")
    def test_poke_dag(self, mock_get_dag_runs_response, get_dag_runs_response, context):
        mock_get_dag_runs_response.return_value = get_dag_runs_response
        sensor = ExternalDeploymentSensor(task_id="test_me", external_dag_id="test_dag")
        response = sensor.poke(context)
        if get_dag_runs_response in [None, []]:
            assert response is True
        elif get_dag_runs_response[0].get("state") == "success":
            assert response is True
        else:
            assert response is False

    @pytest.mark.parametrize("task_state", ["success", "running"])
    @mock.patch("astronomer.providers.core.hooks.astro.AstroHook.get_task_instance")
    @mock.patch("astronomer.providers.core.hooks.astro.AstroHook.get_dag_runs")
    def test_poke_task(self, mock_get_dag_runs_response, mock_get_task_instance, task_state, context):
        mock_get_dag_runs_response.return_value = [{"dag_run_id": "run_id", "state": "running"}]
        mock_get_task_instance.return_value = {"state": task_state}
        sensor = ExternalDeploymentSensor(
            task_id="test_me", external_dag_id="test_dag", external_task_id="task_id"
        )
        response = sensor.poke(context)
        if task_state == "success":
            assert response is True
        else:
            assert response is False

    @pytest.mark.parametrize("poke_response", [True, False])
    @mock.patch("astronomer.providers.core.sensors.astro.ExternalDeploymentSensor.poke")
    def test_execute(self, mock_poke, poke_response, context):
        mock_poke.return_value = poke_response

        sensor = ExternalDeploymentSensor(task_id="test_me", external_dag_id="test_dag")
        if poke_response:
            response = sensor.execute(context)
            assert response is None
        else:
            with pytest.raises(TaskDeferred) as exc:
                sensor.execute(context)
            assert isinstance(
                exc.value.trigger, AstroDeploymentTrigger
            ), "Trigger is not a AstroDeploymentTrigger"

    @pytest.mark.parametrize(
        "event,soft_fail",
        [
            ({"status": "done"}, False),
            ({"status": "done"}, True),
            ({"status": "failed"}, False),
            ({"status": "failed"}, True),
        ],
    )
    def test_execute_complete(self, event, soft_fail, context):
        sensor = ExternalDeploymentSensor(task_id="test_me", external_dag_id="test_dag", soft_fail=soft_fail)

        if soft_fail:
            if event.get("status") == "failed":
                with pytest.raises(AirflowSkipException) as exc:
                    sensor.execute_complete(context, event)
                assert str(exc.value) == "Upstream job failed. Skipping the task."
            if event.get("status") == "done":
                assert sensor.execute_complete(context, event) is None
        else:
            if event.get("status") == "failed":
                with pytest.raises(AirflowException) as exc:
                    sensor.execute_complete(context, event)
                assert str(exc.value) == "Upstream job failed."
            if event.get("status") == "done":
                assert sensor.execute_complete(context, event) is None
