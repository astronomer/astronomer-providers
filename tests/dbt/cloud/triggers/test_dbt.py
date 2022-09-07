import asyncio
import time
from unittest import mock

import pytest
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudJobRunStatus

from astronomer.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger
from tests.utils.config import Config


class TestDbtCloudRunJobTrigger:
    DAG_ID = "dbt_cloud_run"
    TASK_ID = "dbt_cloud_run_task_op"
    RUN_ID = 1234
    CONN_ID = "dbt_cloud_default"
    ACCOUNT_ID = 12340
    END_TIME = time.time() + 60 * 60 * 24 * 7

    def test_serialization(self):
        """Assert DbtCloudRunJobTrigger correctly serializes its arguments and classpath."""
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=Config.POLL_INTERVAL,
            end_time=self.END_TIME,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
            wait_for_termination=True,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.dbt.cloud.triggers.dbt.DbtCloudRunJobTrigger"
        assert kwargs == {
            "run_id": self.RUN_ID,
            "account_id": self.ACCOUNT_ID,
            "conn_id": self.CONN_ID,
            "end_time": self.END_TIME,
            "poll_interval": Config.POLL_INTERVAL,
            "wait_for_termination": True,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_cloud_job_run_status",
        [DbtCloudJobRunStatus.QUEUED, DbtCloudJobRunStatus.STARTING, DbtCloudJobRunStatus.RUNNING],
    )
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_job_status")
    async def test_dbt_run_job_trigger(self, mocked_get_job_status, mock_cloud_job_run_status):
        """Test DbtCloudRunJobTrigger is triggered with mocked details and run successfully."""
        mocked_get_job_status.return_value = mock_cloud_job_run_status
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=Config.POLL_INTERVAL,
            end_time=self.END_TIME,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
            wait_for_termination=True,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is True
        asyncio.get_event_loop().stop()
