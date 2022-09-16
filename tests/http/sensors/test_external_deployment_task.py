from unittest import mock

import pytest
from airflow.exceptions import TaskDeferred

from astronomer.providers.http.sensors.external_deployment_task import (
    ExternalDeploymentTaskSensorAsync,
)
from astronomer.providers.http.triggers.http import ExternalDeploymentTaskTrigger


class TestExternalDeploymentTaskSensorAsync:
    TASK_ID = "test_external_deployment_task"
    CONN_ID = "http_default"
    ENDPOINT = "test-endpoint/"

    def test_external_deployment_run(self):
        """Assert execute method defer for external deployment task run status sensors"""
        task = ExternalDeploymentTaskSensorAsync(
            task_id=self.TASK_ID,
            http_conn_id=self.CONN_ID,
            endpoint=self.ENDPOINT,
            request_params={},
            headers={},
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute({})
        assert isinstance(
            exc.value.trigger, ExternalDeploymentTaskTrigger
        ), "Trigger is not a ExternalDeploymentTaskTrigger"

    @pytest.mark.parametrize(
        "mock_state, mock_message",
        [("success", "Task Succeeded with response: %s")],
    )
    def test_external_deployment_execute_complete_success(self, mock_state, mock_message):
        """Assert execute_complete log success message when trigger fire with target state"""
        task = ExternalDeploymentTaskSensorAsync(
            task_id=self.TASK_ID,
            http_conn_id=self.CONN_ID,
            endpoint=self.ENDPOINT,
            request_params={},
            headers={},
        )

        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context={}, event={"state": mock_state})
        mock_log_info.assert_called_with(mock_message, {"state": mock_state})

    @pytest.mark.parametrize(
        "mock_resp, mock_message",
        [
            ({"state": "error"}, "Task Failed with response: %s"),
            ({"test": "test"}, "Task Failed with response: %s"),
        ],
    )
    def test_external_deployment_execute_complete_value_error(self, mock_resp, mock_message):
        """Assert execute_complete method to raise Value error"""
        task = ExternalDeploymentTaskSensorAsync(
            task_id=self.TASK_ID,
            http_conn_id=self.CONN_ID,
            endpoint=self.ENDPOINT,
            request_params={},
            headers={},
        )
        with pytest.raises(ValueError):
            task.execute_complete(context={}, event=mock_resp)
