from unittest import mock

import pytest
from airflow.exceptions import TaskDeferred

from astronomer_operators.http import HttpSensorAsync, HttpTrigger


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

    with mock.patch(
        "astronomer_operators.http.HttpSensorAsync.defer"
    ) as mock_defer, mock.patch("airflow.sensors.base.BaseSensorOperator.execute"):
        operator.execute({})

    mock_defer.assert_not_called()

    operator = HttpSensorAsync(
        task_id="run_now",
        endpoint="test-endpoint",
    )

    with mock.patch("astronomer_operators.http.HttpSensorAsync.defer") as mock_defer:
        operator.execute({})
        mock_defer.assert_called_once_with(
            timeout=None, trigger=mock.ANY, method_name="execute_complete"
        )


def test_http_trigger_serialization():
    """
    Asserts that the HttpTrigger correctly serializes its arguments and classpath.
    """
    trigger = HttpTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
        method="GET",
        headers={"Content-Type": "application/json"},
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.http.HttpTrigger"
    assert kwargs == {
        "data": None,
        "endpoint": "test-endpoint",
        "extra_options": {},
        "headers": {"Content-Type": "application/json"},
        "http_conn_id": "http_default",
        "poll_interval": 5.0,
    }
