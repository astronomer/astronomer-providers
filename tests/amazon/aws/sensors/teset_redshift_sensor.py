from unittest import mock

import pytest
from airflow.exceptions import AirflowException

from astronomer_operators.amazon.aws.sensors.redshift_cluster_sensor import (
    RedshiftClusterSensorAsync,
)


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_redshift_sensor_async_execute_failure(context):
    """Test RedshiftClusterSensorAsync with an AirflowException is raised in case of error event"""
    task = RedshiftClusterSensorAsync(
        task_id="task-id",
        cluster_identifier="astro-redshift-cluster-1",
        target_status="available",
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_redshift_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = RedshiftClusterSensorAsync(
        task_id="task-id",
        cluster_identifier="astro-redshift-cluster-1",
        target_status="available",
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
    mock_log_info.assert_called_with(True)
