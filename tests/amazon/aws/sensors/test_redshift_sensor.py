from airflow.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensor,
)

from astronomer.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)

TASK_ID = "redshift_sensor_check"


class TestRedshiftClusterSensorAsync:
    def test_init(self):
        task = RedshiftClusterSensorAsync(
            task_id=TASK_ID,
            cluster_identifier="astro-redshift-cluster-1",
            target_status="available",
        )
        assert isinstance(task, RedshiftClusterSensor)
        assert task.deferrable is True
