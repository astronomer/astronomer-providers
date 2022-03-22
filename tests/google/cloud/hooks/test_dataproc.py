from unittest import mock

import pytest

from astronomer.providers.google.cloud.hooks.dataproc import DataprocHookAsync

GCP_LOCATION = "global"
JOB_ID = "test-id"
GCP_PROJECT = "test-project"


@pytest.mark.asyncio
@mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook.get_job_client")
async def test_get_job(mock_client):
    """Test to get the job from Google cloud dataproc"""
    hook = DataprocHookAsync()
    await hook.get_job(region=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
    mock_client.assert_called_once_with(region=GCP_LOCATION)
    mock_client.return_value.get_job.assert_called_once_with(
        request={
            "region": GCP_LOCATION,
            "job_id": JOB_ID,
            "project_id": GCP_PROJECT,
        },
        retry=None,
        timeout=None,
        metadata=(),
    )


@pytest.mark.asyncio
@mock.patch("airflow.providers.google.cloud.hooks.dataproc.DataprocHook.get_job_client")
async def test_get_job_deprecation_warning(mock_client):
    """Test to see if warning is raised if location and region aren't present"""
    hook = DataprocHookAsync()
    warning_message = (
        "Parameter `location` will be deprecated. " "Please provide value through `region` parameter instead."
    )
    with pytest.warns(DeprecationWarning) as warnings:
        await hook.get_job(location=GCP_LOCATION, job_id=JOB_ID, project_id=GCP_PROJECT)
        mock_client.assert_called_once_with(region=GCP_LOCATION)
        mock_client.return_value.get_job.assert_called_once_with(
            request={
                "region": GCP_LOCATION,
                "job_id": JOB_ID,
                "project_id": GCP_PROJECT,
            },
            retry=None,
            timeout=None,
            metadata=(),
        )
        assert warning_message == str(warnings[0].message)

    with pytest.raises(TypeError):
        await hook.get_job(job_id=JOB_ID, project_id=GCP_PROJECT)
