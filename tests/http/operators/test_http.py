from unittest import mock

import pytest
from airflow.exceptions import TaskDeferred

from astronomer.providers.http.sensors.http import HttpSensorAsync
from astronomer.providers.http.triggers.http import HttpTrigger


def test_http_run_now_operator_async():
    """
    Asserts that a task is deferred and a HttpTrigger will be fired
    when the HttpSensorAsync is executed.
    """

    operator = HttpSensorAsync(
        task_id="run_now",
        endpoint="test-endpoint",
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute({})

    assert isinstance(exc.value.trigger, HttpTrigger), "Trigger is not a HttpTrigger"


def test_http_response_check_does_not_run_async():
    """
    Asserts that a task is not deferred when response_check arg is passed to HttpSensorAsync.
    """

    operator = HttpSensorAsync(
        task_id="run_now",
        endpoint="test-endpoint",
        response_check=lambda response: "httpbin" in response.text,
    )

    with mock.patch("astronomer.providers.http.sensors.http.HttpSensorAsync.defer") as mock_defer, mock.patch(
        "airflow.sensors.base.BaseSensorOperator.execute"
    ):
        operator.execute({})

    mock_defer.assert_not_called()

    operator = HttpSensorAsync(
        task_id="run_now",
        endpoint="test-endpoint",
    )

    with mock.patch("astronomer.providers.http.sensors.http.HttpSensorAsync.defer") as mock_defer:
        operator.execute({})
        mock_defer.assert_called_once_with(timeout=None, trigger=mock.ANY, method_name="execute_complete")
