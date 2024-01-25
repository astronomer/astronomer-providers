from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)

from astronomer.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperatorAsync,
    DataprocDeleteClusterOperatorAsync,
    DataprocSubmitJobOperatorAsync,
    DataprocUpdateClusterOperatorAsync,
)

TEST_PROJECT_ID = "test_project_id"
TEST_CLUSTER_NAME = "test_cluster"
TEST_REGION = "us-central1"
TEST_ZONE = "us-central1-a"
TEST_JOB_ID = "test-job"

SPARK_JOB = {
    "reference": {"project_id": TEST_PROJECT_ID},
    "placement": {"cluster_name": TEST_CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["some/random/path"],
        "main_class": "test.main.class",
    },
}

MODULE = "airflow.providers.google.cloud.operators.dataproc.DataprocHook"


class TestDataprocCreateClusterOperatorAsync:
    def test_init(self):
        task = DataprocCreateClusterOperatorAsync(
            task_id="task-id", cluster_name="test_cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        assert isinstance(task, DataprocCreateClusterOperator)
        assert task.deferrable is True


class TestDataprocDeleteClusterOperatorAsync:
    def test_init(self):
        task = DataprocDeleteClusterOperatorAsync(
            task_id="task-id",
            project_id=TEST_PROJECT_ID,
            cluster_name=TEST_CLUSTER_NAME,
            region=TEST_REGION,
            timeout=None,
        )
        assert isinstance(task, DataprocDeleteClusterOperator)
        assert task.deferrable is True


class TestDataprocSubmitJobOperatorAsync:
    def test_init(self):
        task = DataprocSubmitJobOperatorAsync(
            task_id="task-id", job=SPARK_JOB, region=TEST_REGION, project_id=TEST_PROJECT_ID
        )

        assert isinstance(task, DataprocSubmitJobOperator)
        assert task.deferrable is True


class TestDataprocUpdateClusterOperatorAsync:
    def test_init(self):
        task = DataprocUpdateClusterOperatorAsync(
            task_id="task-id",
            cluster_name="test_cluster",
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
            cluster={},
            graceful_decommission_timeout=30,
            update_mask={},
        )
        assert isinstance(task, DataprocUpdateClusterOperator)
        assert task.deferrable is True
