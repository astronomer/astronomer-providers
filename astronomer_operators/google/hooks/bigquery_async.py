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
from asgiref.sync import sync_to_async
from gcloud.aio.bigquery import Job
from google.cloud.bigquery import CopyJob, ExtractJob, LoadJob, QueryJob
from hooks.base import BaseHook

from astronomer_operators.google.hooks.bigquery import BigQueryHook

BigQueryJob = Union[CopyJob, QueryJob, LoadJob, ExtractJob]


class BigQueryHookAsync(BaseHook):
    def __init__(self, **kwargs):
        self._hook_kwargs = kwargs
        self._bigquery_hook_sync = None

    async def get_bigquery_hook_sync(self):
        """
        Sync version of the BigQueryHook makes blocking calls in ``__init__`` so we don't inherit
        from it but expose it here as a cached property.
        """
        if not self._bigquery_hook_sync:
            self._bigquery_hook_sync = await sync_to_async(BigQueryHook)(**self._hook_kwargs)
            print("initialized sync version of hook")
        return self._bigquery_hook_sync

    async def service_file_as_context(self):
        sync_hook = await self.get_bigquery_hook_sync()
        return await sync_to_async(sync_hook.provide_gcp_credential_file_as_context)()

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
