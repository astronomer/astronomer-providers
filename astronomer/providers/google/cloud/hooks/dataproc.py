import warnings
from typing import Any, Optional, Sequence, Tuple, Union

from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from asgiref.sync import sync_to_async
from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import Job

JobType = Union[Job, Any]


class DataprocHookAsync(DataprocHook):
    """
    Async Hook for Google Cloud Dataproc APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_job(  # type: ignore[override]
        self,
        job_id: str,
        project_id: str,
        region: Optional[str] = None,
        location: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> JobType:
        """
        Gets the resource representation for a job in a project.

        :param job_id: Id of the Dataproc job
        :param project_id: Required. The ID of the Google Cloud project the cluster belongs to.
        :param region: Required. The Cloud Dataproc region in which to handle the request.
        :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request.
        :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
            retried.
        :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
            ``retry`` is specified, the timeout applies to each individual attempt.
        :param metadata: Additional metadata that is provided to the method.
        """
        if region is None:
            if location is not None:
                warnings.warn(
                    "Parameter `location` will be deprecated. "
                    "Please provide value through `region` parameter instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                region = location
            else:
                raise TypeError("missing 1 required keyword argument: 'region'")
        client = await sync_to_async(self.get_job_client)(region=region)
        job = client.get_job(
            request={"project_id": project_id, "region": region, "job_id": job_id},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return job
