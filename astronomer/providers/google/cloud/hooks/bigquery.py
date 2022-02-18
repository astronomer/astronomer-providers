#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains a BigQueryHookAsync
"""
from typing import Dict, Optional, Union

from aiohttp import ClientSession as Session
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from gcloud.aio.bigquery import Job
from google.cloud.bigquery import CopyJob, ExtractJob, LoadJob, QueryJob

from astronomer.providers.google.common.hooks.base_google import GoogleBaseHookAsync

BigQueryJob = Union[CopyJob, QueryJob, LoadJob, ExtractJob]


class _BigQueryHook(BigQueryHook):
    @GoogleBaseHook.fallback_to_default_project_id
    def insert_job(
        self,
        configuration: Dict,
        job_id: Optional[str] = None,
        project_id: Optional[str] = None,
        location: Optional[str] = None,
        nowait: bool = False,
    ) -> BigQueryJob:
        """
        Executes a BigQuery job. Initiates the job and returns job id.
        See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        :param job_id: The ID of the job. The ID must contain only letters (a-z, A-Z),
            numbers (0-9), underscores (_), or dashes (-). The maximum length is 1,024
            characters. If not provided then uuid will be generated.
        :param project_id: Google Cloud Project where the job is running
        :param location: location the job is running
        :param nowait: specify whether to insert job without waiting for the result
        """
        location = location or self.location
        job_id = job_id or self._custom_job_id(configuration)

        client = self.get_client(project_id=project_id, location=location)
        job_data = {
            "configuration": configuration,
            "jobReference": {"jobId": job_id, "projectId": project_id, "location": location},
        }

        supported_jobs = {
            LoadJob._JOB_TYPE: LoadJob,
            CopyJob._JOB_TYPE: CopyJob,
            ExtractJob._JOB_TYPE: ExtractJob,
            QueryJob._JOB_TYPE: QueryJob,
        }

        job = None
        for job_type, job_object in supported_jobs.items():
            if job_type in configuration:
                job = job_object
                break

        if not job:
            raise AirflowException(f"Unknown job type. Supported types: {supported_jobs.keys()}")
        job = job.from_api_repr(job_data, client)
        self.log.info("Inserting job %s", job.job_id)
        if nowait:
            # Initiate the job and don't wait for it to complete.
            job._begin()
        else:
            # Start the job and wait for it to complete and get the result.
            job.result()
        return job


class BigQueryHookAsync(GoogleBaseHookAsync):
    sync_hook_class = _BigQueryHook

    async def get_job_instance(self, project_id, job_id, session) -> Job:
        """Get the specified job resource by job ID and project ID."""
        with await self.service_file_as_context() as f:
            return Job(job_id=job_id, project=project_id, service_file=f, session=session)

    async def get_job_status(
        self,
        job_id: str,
        project_id: Optional[str] = None,
    ):
        """Polls for job status asynchronously using gcloud-aio.
        Note that an OSError is raised when Job results are still pending.
        Exception means that Job finished with errors"""
        async with Session() as s:
            try:
                self.log.info("Executing get_job_status...")
                job_client = await self.get_job_instance(project_id, job_id, s)
                job_status_response = await job_client.result(s)
                if job_status_response:
                    job_status = "success"
            except OSError:
                job_status = "pending"
            except Exception as e:
                self.log.info("Query execution finished with errors...")
                job_status = str(e)
            return job_status
