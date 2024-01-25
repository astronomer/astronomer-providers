
import pytest
from airflow.providers.http.sensors.http import HttpSensor

from astronomer.providers.http.sensors.http import HttpSensorAsync

MODULE = "astronomer.providers.http.sensors.http"


class TestHttpSensorAsync:
    def test_init(self):
        task = HttpSensorAsync(HttpSensorAsync(task_id="run_now", endpoint="test-endpoint"))
        assert isinstance(task, HttpSensor)
        assert task.deferrable is True

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for HttpSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            HttpSensorAsync(task_id="check_hook_initialisation", endpoint="", poll_interval=5.0)
