from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor, EmrJobFlowSensor, EmrStepSensor

from astronomer.providers.amazon.aws.sensors.emr import (
    EmrContainerSensorAsync,
    EmrJobFlowSensorAsync,
    EmrStepSensorAsync,
)

TASK_ID = "test_emr_container_sensor"
VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "j-T0CT8Z0C20NT"
AWS_CONN_ID = "aws_default"
STEP_ID = "s-34RJO0CKERRPL"

MODULE = "astronomer.providers.amazon.aws.sensors.emr"


MODULE = "astronomer.providers.amazon.aws.sensors.emr"


class TestEmrContainerSensorAsync:
    def test_init(self):
        task = EmrContainerSensorAsync(
            task_id=TASK_ID,
            virtual_cluster_id=VIRTUAL_CLUSTER_ID,
            job_id=JOB_ID,
            poll_interval=5,
            max_retries=1,
            aws_conn_id=AWS_CONN_ID,
        )
        assert isinstance(task, EmrContainerSensor)
        assert task.deferrable is True


class TestEmrStepSensorAsync:
    def test_init(self):
        task = EmrStepSensorAsync(
            task_id="emr_step_sensor",
            job_flow_id=JOB_ID,
            step_id=STEP_ID,
        )
        assert isinstance(task, EmrStepSensor)
        assert task.deferrable is True


class TestEmrJobFlowSensorAsync:
    def test_init(self):
        task = EmrJobFlowSensorAsync(
            task_id=TASK_ID,
            job_flow_id=JOB_ID,
        )
        assert isinstance(task, EmrJobFlowSensor)
        assert task.deferrable is True
