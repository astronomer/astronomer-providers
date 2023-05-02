from unittest import mock

import pytest
from airflow.exceptions import TaskDeferred

from astronomer.providers.http.sensors.http import HttpSensorAsync
from astronomer.providers.http.triggers.http import HttpTrigger

MODULE = "astronomer.providers.http.sensors.http"


class TestHttpSensorAsync:
    @mock.patch(f"{MODULE}.HttpSensorAsync.defer")
    @mock.patch(
        f"{MODULE}.HttpSensorAsync.poke",
        return_value=True,
    )
    def test_http_sensor_async_finish_before_deferred(
        self,
        mock_poke,
        mock_defer,
    ):
        """
        Asserts that a task is deferred and a HttpTrigger will be fired
        when the HttpSensorAsync is executed.
        """

        task = HttpSensorAsync(
            task_id="run_now",
            endpoint="test-endpoint",
        )

        task.execute({})
        assert not mock_defer.called

    @mock.patch(
        f"{MODULE}.HttpSensorAsync.poke",
        return_value=False,
    )
    def test_http_run_now_sensor_async(self, mock_poke):
        """
        Asserts that a task is deferred and a HttpTrigger will be fired
        when the HttpSensorAsync is executed.
        """

        task = HttpSensorAsync(
            task_id="run_now",
            endpoint="test-endpoint",
        )

        with pytest.raises(TaskDeferred) as exc:
            task.execute({})

        assert isinstance(exc.value.trigger, HttpTrigger), "Trigger is not a HttpTrigger"

    @mock.patch("airflow.sensors.base.BaseSensorOperator.execute")
    def test_sensor_not_defer(self, mock_execute):
        task = HttpSensorAsync(
            task_id="run_now",
            endpoint="test-endpoint",
            response_check=lambda response: "httpbin" in response.text,
        )
        task.execute({})
        mock_execute.assert_called_once()

    @mock.patch("airflow.providers.http.sensors.http.HttpSensor.poke")
    def test_sensor_defer(self, mock_poke):
        mock_poke.return_value = False
        task = HttpSensorAsync(task_id="run_now", endpoint="test-endpoint")
        with pytest.raises(TaskDeferred) as exc:
            task.execute({})
        assert isinstance(exc.value.trigger, HttpTrigger), "Trigger is not a HttpTrigger"

    @mock.patch("astronomer.providers.http.sensors.http.HttpSensor")
    def test_http_sensor_async_hook_initialisation_attribute_error(self, mock_http_sensor):
        """
        Asserts that an attribute error that may be raised across different versions of the HTTP provider is handled
        while initialising the hook in the sensor.
        """
        mock_http_sensor.side_effect = AttributeError()
        HttpSensorAsync(task_id="check_hook_initialisation", endpoint="")

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for HttpSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            HttpSensorAsync(task_id="check_hook_initialisation", endpoint="", poll_interval=5.0)
