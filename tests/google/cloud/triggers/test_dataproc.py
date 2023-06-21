import asyncio
import time
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.triggers.base import TriggerEvent
from google.api_core.exceptions import NotFound
from google.cloud import dataproc
from google.cloud.dataproc_v1 import Cluster, Job
from google.cloud.dataproc_v1.types import JobStatus

from astronomer.providers.google.cloud.hooks.dataproc import DataprocHookAsync
from astronomer.providers.google.cloud.triggers.dataproc import (
    DataprocCreateClusterTrigger,
    DataprocDeleteClusterTrigger,
    DataProcSubmitTrigger,
)

TEST_PROJECT_ID = "test_project_id"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_CLUSTER_NAME = "test_ccluster"
TEST_REGION = "us-central1"
TEST_JOB_ID = "test-job"
TEST_POLLING_INTERVAL = 3.0
TEST_IMPERSONATION_CHAIN = None


class TestDataprocCreateClusterTrigger:
    def test_dataproc_create_cluster_trigger_serialization(self):
        """
        asserts that the DataprocCreateClusterTrigger correctly serializes its arguments and classpath.
        """
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            gcp_conn_id=TEST_GCP_CONN_ID,
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=100,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger"
        assert kwargs == {
            "project_id": TEST_PROJECT_ID,
            "region": TEST_REGION,
            "cluster_name": "test_cluster",
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "polling_interval": TEST_POLLING_INTERVAL,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "delete_on_error": True,
            "labels": None,
            "cluster_config": None,
            "end_time": 100,
            "metadata": (),
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_run_running(self, mock_get_cluster):
        """assert that run method yield correctly when cluster is running"""
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.RUNNING),
        )
        mock_get_cluster.return_value = cluster

        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent(
            {"status": "success", "data": Cluster.to_dict(cluster), "cluster_name": "test_cluster"}
        )

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_run_pending(self, mock_get_cluster):
        """assert that run method wait when cluster being is creating"""
        mock_get_cluster.return_value = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.CREATING),
        )

        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_run_exception(self, mock_get_cluster):
        """assert that run method raise exception when get_cluster call fail"""
        mock_get_cluster.side_effect = Exception("Fail to fetch cluster")
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "error", "message": "Fail to fetch cluster"})

    @pytest.mark.asyncio
    async def test_run_timeout(self):
        """assert that run method timeout when end_time > start time"""
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() - 100,
        )

        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "error", "message": "Timeout"})

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook.create_cluster")
    def test__create_cluster(self, mock_create_cluster):
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() - 100,
        )
        trigger._create_cluster()

        mock_create_cluster.assert_called_once_with(
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
            cluster_name=TEST_CLUSTER_NAME,
            cluster_config=None,
            labels=None,
            metadata=(),
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook.diagnose_cluster")
    def test__diagnose_cluster(self, mock_diagnose_cluster):
        """Assert diagnose_cluster call with correct param"""
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() - 100,
        )
        trigger._diagnose_cluster()

        mock_diagnose_cluster.assert_called_once_with(
            region=TEST_REGION, project_id=TEST_PROJECT_ID, cluster_name=TEST_CLUSTER_NAME, metadata=()
        )

    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook.delete_cluster")
    def test__delete_cluster(self, mock_delete_cluster):
        """Assert delete_cluster call with correct param"""
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name=TEST_CLUSTER_NAME,
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() - 100,
        )
        trigger._delete_cluster()

        mock_delete_cluster.assert_called_once_with(
            region=TEST_REGION, project_id=TEST_PROJECT_ID, cluster_name=TEST_CLUSTER_NAME, metadata=()
        )

    @pytest.mark.asyncio
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._get_cluster"
    )
    async def test__wait_for_deleting(self, mock_get_cluster):
        """Assert that _wait_for_deleting wait if cluster status is deleting"""
        mock_get_cluster.return_value = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.DELETING),
        )

        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )

        task = asyncio.create_task(trigger._wait_for_deleting())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._get_cluster"
    )
    async def test_wait_for_deleting_success(self, mock__get_cluster):
        """Assert that _wait_for_deleting return success if cluster not found exception raise by get_cluster"""
        mock__get_cluster.side_effect = NotFound("Cluster deleted")
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )
        assert await trigger._wait_for_deleting() is None

    @pytest.mark.asyncio
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._get_cluster"
    )
    async def test_wait_for_deleting_exception(self, mock__get_cluster):
        """Assert that _wait_for_deleting return raise exception when get_cluster raise exception"""
        mock__get_cluster.side_effect = Exception("Error occur while deleting")
        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )
        with pytest.raises(Exception):
            await trigger._wait_for_deleting()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "delete_on_error",
        [True, False],
    )
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._delete_cluster"
    )
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._wait_for_deleting"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook.diagnose_cluster")
    async def test__handle_error(
        self, mock_diagnose_cluster, mock_wait_for_deleting, mock_delete_cluster, delete_on_error
    ):
        """Assert that _handle_error raise exception correctly in case of error"""
        mock_diagnose_cluster.return_value = {}
        mock_delete_cluster.return_value = {}
        mock_wait_for_deleting.return_value = {}
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.ERROR),
        )

        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
            delete_on_error=delete_on_error,
        )
        with pytest.raises(AirflowException):
            await trigger._handle_error(cluster=cluster)

    @pytest.mark.asyncio
    async def test__handle_error_with_no_error(self):
        """Assert that _handle_error return success when no error"""
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.DELETING),
        )

        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )

        assert await trigger._handle_error(cluster=cluster) is None

    @pytest.mark.asyncio
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._handle_error"
    )
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._create_cluster"
    )
    @mock.patch(
        "astronomer.providers.google.cloud.triggers.dataproc.DataprocCreateClusterTrigger._wait_for_deleting"
    )
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_run_deleting(
        self, mock_get_cluster, mock_wait_for_deleting, mock_create_cluster, mock_handle_error
    ):
        """
        assert that run method call
        1. _wait_for_deleting correctly
        2. _create_cluster
        3. _handle_error
        methods correctly when cluster status is deleting
        """
        cluster = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.DELETING),
        )
        mock_get_cluster.return_value = cluster

        trigger = DataprocCreateClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )
        asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        mock_wait_for_deleting.assert_called_once_with()
        mock_create_cluster.assert_called_once_with()
        mock_handle_error.assert_called_once_with(cluster)
        asyncio.get_event_loop().stop()


class TestDataProcSubmitTrigger:
    def test_dataproc_submit_trigger_serialization(self):
        """
        Asserts that the DataProcSubmitTrigger correctly serializes its arguments and classpath.
        """
        trigger = DataProcSubmitTrigger(
            gcp_conn_id=TEST_GCP_CONN_ID,
            dataproc_job_id=TEST_JOB_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            polling_interval=TEST_POLLING_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger"
        assert kwargs == {
            "project_id": TEST_PROJECT_ID,
            "dataproc_job_id": TEST_JOB_ID,
            "region": TEST_REGION,
            "polling_interval": TEST_POLLING_INTERVAL,
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [
            ({"status": "success", "message": "Job completed successfully"}),
            ({"status": "error", "message": "Job Failed"}),
        ],
    )
    @mock.patch("astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger._get_job_status")
    async def test_dataproc_submit_return_success_and_failure(self, mock_get_job_status, status):
        """Tests that the DataProcSubmitTrigger is success case and also error case"""
        mock_get_job_status.return_value = status

        trigger = DataProcSubmitTrigger(
            dataproc_job_id=TEST_JOB_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            polling_interval=TEST_POLLING_INTERVAL,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent(status) == actual

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger._get_job_status")
    async def test_dataproc_submit_return_pending(self, mock_get_job_status):
        """Tests that the DataProcSubmitTrigger is in pending state"""
        mock_get_job_status.return_value = {"status": "pending"}

        trigger = DataProcSubmitTrigger(
            dataproc_job_id=TEST_JOB_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            polling_interval=TEST_POLLING_INTERVAL,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger._get_job_status")
    async def test_dataproc_submit_return_exception(self, mock_get_job_status):
        """Tests that the DataProcSubmitTrigger throws an exception"""
        mock_get_job_status.side_effect = Exception("Test exception")

        trigger = DataProcSubmitTrigger(
            dataproc_job_id=TEST_JOB_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            polling_interval=TEST_POLLING_INTERVAL,
        )
        task = [i async for i in trigger.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "state, response",
        [
            (
                JobStatus.State.DONE,
                {"status": "success", "message": "Job completed successfully", "job_id": TEST_JOB_ID},
            ),
            (JobStatus.State.ERROR, {"status": "error", "message": "Job Failed", "job_id": TEST_JOB_ID}),
            (
                JobStatus.State.CANCELLED,
                {"status": "error", "message": "Job got cancelled", "job_id": TEST_JOB_ID},
            ),
            (
                JobStatus.State.ATTEMPT_FAILURE,
                {"status": "pending", "message": "Job is in pending state", "job_id": TEST_JOB_ID},
            ),
            (
                JobStatus.State.SETUP_DONE,
                {"status": "pending", "message": "Job is in pending state", "job_id": TEST_JOB_ID},
            ),
        ],
    )
    async def test_dataproc_get_job_status(self, state, response):
        """Tests that the get job status gives appropriate status for the job"""
        hook = mock.AsyncMock(DataprocHookAsync)
        get_job_instance = mock.AsyncMock(Job)
        hook.get_job = get_job_instance
        job = hook.get_job.return_value
        response["message"] = f"{response['message']}.\n {job}"
        job.status.state = state
        trigger = DataProcSubmitTrigger(
            dataproc_job_id=TEST_JOB_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            polling_interval=TEST_POLLING_INTERVAL,
        )
        res = await trigger._get_job_status(hook)
        assert res == response


class TestDataprocDeleteClusterTrigger:
    def test_dataproc_delete_cluster_trigger_serialization(self):
        """
        asserts that the DataprocDeleteClusterTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = DataprocDeleteClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            gcp_conn_id=TEST_GCP_CONN_ID,
            polling_interval=TEST_POLLING_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            end_time=100,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.google.cloud.triggers.dataproc.DataprocDeleteClusterTrigger"
        assert kwargs == {
            "project_id": TEST_PROJECT_ID,
            "region": TEST_REGION,
            "cluster_name": "test_cluster",
            "gcp_conn_id": TEST_GCP_CONN_ID,
            "polling_interval": TEST_POLLING_INTERVAL,
            "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            "end_time": 100,
            "metadata": (),
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_delete_cluster_run_pending(self, mock_get_cluster):
        """assert that run method wait when cluster being is deleting"""
        mock_get_cluster.return_value = Cluster(
            cluster_name="test_cluster",
            status=dataproc.ClusterStatus(state=dataproc.ClusterStatus.State.DELETING),
        )

        trigger = DataprocDeleteClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_delete_run_success(self, mock_get_cluster):
        """assert that run method yield correctly when cluster is deleted"""
        mock_get_cluster.side_effect = NotFound("Cluster deleted")

        trigger = DataprocDeleteClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "success", "message": ""})

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_delete_run_exception(self, mock_get_cluster):
        """assert that run method raise exception when get_cluster call fail"""
        mock_get_cluster.side_effect = Exception("Cluster deletion fail")

        trigger = DataprocDeleteClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() + 100,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "error", "message": "Cluster deletion fail"})

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.hooks.dataproc.DataprocHookAsync.get_cluster")
    async def test_delete_run_timeout(self, mock_get_cluster):
        """assert that run method timeout when end_time > start time"""
        mock_get_cluster.side_effect = Exception("Cluster deletion fail")

        trigger = DataprocDeleteClusterTrigger(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            cluster_name="test_cluster",
            polling_interval=TEST_POLLING_INTERVAL,
            end_time=time.time() - 100,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert actual == TriggerEvent({"status": "error", "message": "Timeout"})
