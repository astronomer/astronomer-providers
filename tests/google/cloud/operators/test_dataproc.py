from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.google.cloud.hooks.bigquery import NotFound
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from google.api_core.exceptions import AlreadyExists
from google.cloud import dataproc
from google.cloud.dataproc_v1 import Cluster

from astronomer.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperatorAsync,
    DataprocDeleteClusterOperatorAsync,
    DataprocSubmitJobOperatorAsync,
    DataprocUpdateClusterOperatorAsync,
)
from astronomer.providers.google.cloud.triggers.dataproc import (
    DataprocCreateClusterTrigger,
    DataprocDeleteClusterTrigger,
)
from tests.utils.airflow_util import create_context

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
    @mock.patch(
        "astronomer.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperatorAsync.defer"
    )
    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.create_cluster")
    def test_dataproc_operator_create_cluster_execute_async_finish_before_defer(
        self, mock_create_cluster, mock_get_cluster, mock_defer
    ):
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.RUNNING),
        )
        mock_create_cluster.return_value = cluster
        mock_get_cluster.return_value = cluster
        task = DataprocCreateClusterOperatorAsync(
            task_id="task-id", cluster_name="test_cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        task.execute(create_context(task))
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.create_cluster")
    def test_dataproc_operator_create_cluster_execute_async(self, mock_create_cluster, mock_get_cluster):
        """
        Asserts that a task is deferred and a DataprocCreateClusterTrigger will be fired
        when the DataprocCreateClusterOperatorAsync is executed.
        """
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.CREATING),
        )
        mock_create_cluster.return_value = cluster
        mock_get_cluster.return_value = cluster

        task = DataprocCreateClusterOperatorAsync(
            task_id="task-id", cluster_name="test_cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(create_context(task))
        assert isinstance(
            exc.value.trigger, DataprocCreateClusterTrigger
        ), "Trigger is not a DataprocCreateClusterTrigger"

    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.create_cluster")
    def test_dataproc_operator_create_cluster_execute_async_cluster_exist_exception(
        self, mock_create_cluster, mock_get_cluster
    ):
        """
        Asserts that a task will raise exception when dataproc cluster already exist
        and use_if_exists param is False
        """
        mock_create_cluster.side_effect = AlreadyExists("Cluster already exist")
        mock_get_cluster.return_value = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.CREATING),
        )

        task = DataprocCreateClusterOperatorAsync(
            task_id="task-id",
            cluster_name="test_cluster",
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
            use_if_exists=False,
        )
        with pytest.raises(AlreadyExists):
            task.execute(create_context(task))

    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.create_cluster")
    def test_dataproc_operator_create_cluster_execute_async_cluster_exist(
        self, mock_create_cluster, mock_get_cluster
    ):
        """
        Asserts that a task is deferred and a DataprocCreateClusterTrigger will be fired
        when the DataprocCreateClusterOperatorAsync is executed when dataproc cluster already exist.
        """
        mock_create_cluster.side_effect = AlreadyExists("Cluster already exist")
        mock_get_cluster.return_value = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.CREATING),
        )

        task = DataprocCreateClusterOperatorAsync(
            task_id="task-id", cluster_name="test_cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(create_context(task))
        assert isinstance(
            exc.value.trigger, DataprocCreateClusterTrigger
        ), "Trigger is not a DataprocCreateClusterTrigger"

    def test_dataproc_operator_create_cluster_execute_complete_success(self, context):
        """assert that execute_complete return cluster detail when task succeed"""
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.CREATING),
        )
        task = DataprocCreateClusterOperatorAsync(
            task_id="task-id", cluster_name="test_cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        cluster_details = task.execute_complete(
            context=context, event={"status": "success", "data": cluster, "message": ""}
        )
        assert cluster_details is not None

    @pytest.mark.parametrize(
        "status",
        [
            "error",
            None,
        ],
    )
    def test_dataproc_operator_create_cluster_execute_complete_fail(self, status, context):
        """assert that execute_complete raise exception when task fail"""
        task = DataprocCreateClusterOperatorAsync(
            task_id="task-id", cluster_name="test_cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context=context, event={"status": status, "message": "fail to create cluster"}
            )


class TestDataprocDeleteClusterOperatorAsync:
    OPERATOR = DataprocDeleteClusterOperatorAsync(
        task_id="task-id", project_id=TEST_PROJECT_ID, cluster_name=TEST_CLUSTER_NAME, region=TEST_REGION
    )

    @mock.patch(
        "astronomer.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperatorAsync.defer"
    )
    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.delete_cluster")
    def test_dataproc_operator_create_cluster_execute_async_finish_before_defer(
        self, mock_delete_cluster, mock_get_cluster, mock_defer, context
    ):
        mock_delete_cluster.return_value = {}
        mock_get_cluster.side_effect = NotFound("test")
        self.OPERATOR.execute(context)
        assert not mock_defer.called

    @mock.patch(
        "astronomer.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperatorAsync.defer"
    )
    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.delete_cluster")
    def test_dataproc_operator_create_cluster_execute_async_unexpected_error_before_defer(
        self, mock_delete_cluster, mock_get_cluster, mock_defer, context
    ):
        mock_delete_cluster.return_value = {}
        mock_get_cluster.side_effect = Exception("Unexpected")
        with pytest.raises(AirflowException):
            self.OPERATOR.execute(context)
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.delete_cluster")
    def test_dataproc_delete_operator_execute_async(self, mock_delete_cluster, get_cluster, context):
        """
        Asserts that a task is deferred and a DataprocDeleteClusterTrigger will be fired
        when the DataprocDeleteClusterOperatorAsync is executed.
        """
        mock_delete_cluster.return_value = {}
        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(context)
        assert isinstance(
            exc.value.trigger, DataprocDeleteClusterTrigger
        ), "Trigger is not a DataprocDeleteClusterTrigger"

    def test_dataproc_delete_operator_execute_complete_success(self, context):
        """assert that execute_complete execute without error when receive success signal from trigger"""
        assert self.OPERATOR.execute_complete(context=context, event={"status": "success"}) is None

    @pytest.mark.parametrize(
        "event",
        [
            ({"status": "error", "message": "test failure message"}),
            None,
        ],
    )
    def test_dataproc_delete_operator_execute_complete_exception(self, event, context):
        """assert that execute_complete raise exception when receive error from trigger"""
        with pytest.raises(AirflowException):
            self.OPERATOR.execute_complete(context=context, event=event)


class TestDataprocSubmitJobOperatorAsync:
    def test_init(self):
        task = DataprocSubmitJobOperatorAsync(
            task_id="task-id", job=SPARK_JOB, region=TEST_REGION, project_id=TEST_PROJECT_ID
        )

        assert isinstance(task, DataprocSubmitJobOperator)
        assert task.deferrable is True


class TestDataprocUpdateClusterOperatorAsync:
    OPERATOR = DataprocUpdateClusterOperatorAsync(
        task_id="task-id",
        cluster_name="test_cluster",
        region=TEST_REGION,
        project_id=TEST_PROJECT_ID,
        cluster={},
        graceful_decommission_timeout=30,
        update_mask={},
    )

    @mock.patch(
        "astronomer.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperatorAsync.defer"
    )
    @mock.patch("airflow.providers.google.cloud.links.dataproc.DataprocLink.persist")
    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.update_cluster")
    def test_dataproc_operator_update_cluster_execute_async_finish_before_defer(
        self, mock_update_cluster, mock_get_cluster, mock_persist, mock_defer, context
    ):
        mock_persist.return_value = {}
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.RUNNING),
        )
        mock_update_cluster.return_value = cluster
        mock_get_cluster.return_value = cluster
        DataprocCreateClusterOperatorAsync(
            task_id="task-id", cluster_name="test_cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        self.OPERATOR.execute(context)
        assert not mock_defer.called

    @mock.patch("airflow.providers.google.cloud.links.dataproc.DataprocLink.persist")
    @mock.patch(f"{MODULE}.get_cluster")
    @mock.patch(f"{MODULE}.update_cluster")
    def test_dataproc_operator_update_cluster_execute_async(
        self, mock_update_cluster, mock_get_cluster, mock_persist, context
    ):
        """
        Asserts that a task is deferred and a DataprocCreateClusterTrigger will be fired
        when the DataprocCreateClusterOperatorAsync is executed.
        """
        mock_persist.return_value = {}
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.CREATING),
        )
        mock_update_cluster.return_value = cluster
        mock_get_cluster.return_value = cluster

        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(context)
        assert isinstance(
            exc.value.trigger, DataprocCreateClusterTrigger
        ), "Trigger is not a DataprocCreateClusterTrigger"

    def test_dataproc_operator_update_cluster_execute_complete_success(self, context):
        """assert that execute_complete return cluster detail when task succeed"""
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.CREATING),
        )

        assert (
            self.OPERATOR.execute_complete(
                context=context, event={"status": "success", "data": cluster, "cluster_name": "test_cluster"}
            )
            is None
        )

    @pytest.mark.parametrize(
        "event",
        [
            {"status": "error", "message": ""},
            None,
        ],
    )
    def test_dataproc_operator_update_cluster_execute_complete_fail(self, event, context):
        """assert that execute_complete raise exception when task fail"""

        with pytest.raises(AirflowException):
            self.OPERATOR.execute_complete(context=context, event=event)
