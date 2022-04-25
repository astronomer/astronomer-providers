import asyncio
import logging
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent
from google.cloud.dataproc_v1.types import JobStatus

from astronomer.providers.google.cloud.hooks.dataproc import DataprocHookAsync

log = logging.getLogger(__name__)


class DataProcSubmitTrigger(BaseTrigger):
    """
    Check for the state of a previously submitted Dataproc job.

    :param dataproc_job_id: The Dataproc job ID to poll. (templated)
    :param region: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :param wait_timeout: How many seconds wait for job to be ready.
    """

    def __init__(
        self,
        *,
        dataproc_job_id: str,
        region: Optional[str] = None,
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        polling_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataproc_job_id = dataproc_job_id
        self.region = region
        self.polling_interval = polling_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes DataProcSubmitTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger",
            {
                "project_id": self.project_id,
                "dataproc_job_id": self.dataproc_job_id,
                "region": self.region,
                "polling_interval": self.polling_interval,
                "gcp_conn_id": self.gcp_conn_id,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the job running on Google Cloud DataProc is completed or not"""
        try:
            hook = DataprocHookAsync(gcp_conn_id=self.gcp_conn_id)
            while True:
                job_status = await self._get_job_status(hook)
                if "status" in job_status and job_status["status"] == "success":
                    yield TriggerEvent(job_status)
                elif "status" in job_status and job_status["status"] == "error":
                    yield TriggerEvent(job_status)
                await asyncio.sleep(self.polling_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    async def _get_job_status(self, hook: DataprocHookAsync) -> Dict[str, str]:
        """Gets the status of the given job_id from the Google Cloud DataProc"""
        job = await hook.get_job(job_id=self.dataproc_job_id, region=self.region, project_id=self.project_id)
        state = job.status.state
        if state == JobStatus.State.ERROR:
            return {"status": "error", "message": "Job Failed"}
        elif state in {
            JobStatus.State.CANCELLED,
            JobStatus.State.CANCEL_PENDING,
            JobStatus.State.CANCEL_STARTED,
        }:
            return {"status": "error", "message": "Job got cancelled"}
        elif JobStatus.State.DONE == state:
            return {"status": "success", "message": "Job completed successfully"}
        elif JobStatus.State.ATTEMPT_FAILURE == state:
            return {"status": "pending", "message": "Job is in pending state"}
        return {"status": "pending", "message": "Job is in pending state"}
