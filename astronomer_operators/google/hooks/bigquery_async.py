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
from typing import Optional, Union

from aiohttp import ClientSession as Session
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from asgiref.sync import sync_to_async
from gcloud.aio.bigquery import Job
from google.cloud.bigquery import CopyJob, ExtractJob, LoadJob, QueryJob

# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from astronomer_operators.google.hooks.bigquery import BigQueryHook

BigQueryJob = Union[CopyJob, QueryJob, LoadJob, ExtractJob]


class BigQueryHookAsync(BigQueryHook, GoogleBaseHook):
    async def get_job_instance(self, project_id, job_id, s) -> Job:
        """Get the specified job resource by job ID and project ID."""
        with await sync_to_async(self.provide_gcp_credential_file_as_context)() as conn:
            return Job(job_id=job_id, project=project_id, service_file=conn, session=s)

    @GoogleBaseHook.fallback_to_default_project_id
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
