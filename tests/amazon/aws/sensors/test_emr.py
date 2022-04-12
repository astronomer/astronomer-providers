from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.sensors.emr import EmrStepSensorAsync
from astronomer.providers.amazon.aws.triggers.emr import EmrStepSensorTrigger

JOB_FLOW_ID = "j-T0CT8Z0C20NT"
STEP_ID = "s-34RJO0CKERRPL"


@pytest.fixture
def context():
    """Creates an empty context."""
    context = {}
    yield context


def test_emr_step_sensor_async():
    """Assert execute method defer for EmrStepSensorAsync sensor"""
    task = EmrStepSensorAsync(
        task_id="test_execute",
        job_flow_id=JOB_FLOW_ID,
        step_id=STEP_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, EmrStepSensorTrigger), "Trigger is not a EmrStepSensorTrigger"


def test_emr_step_sensor_execute_complete_success():
    """Assert execute_complete log success message when triggerer fire with target state"""
    task = EmrStepSensorAsync(
        task_id="test_execute_complete",
        job_flow_id=JOB_FLOW_ID,
        step_id=STEP_ID,
    )

    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(
            context={}, event={"status": "success", "message": "Job flow currently COMPLETED"}
        )
    mock_log_info.assert_called_with("%s completed successfully.", "j-T0CT8Z0C20NT")


def test_emr_step_sensor_execute_complete_failure():
    """Assert execute_complete method fail"""
    task = EmrStepSensorAsync(
        task_id="test_execute_complete",
        job_flow_id=JOB_FLOW_ID,
        step_id=STEP_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context={}, event={"status": "error", "message": ""})
