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
import warnings
from typing import Dict, Optional, Sequence, Union

from aiohttp import ClientSession as Session
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from gcloud.aio.bigquery import Job
from google.cloud.bigquery import CopyJob, ExtractJob, LoadJob, QueryJob

BigQueryJob = Union[CopyJob, QueryJob, LoadJob, ExtractJob]


class BigQueryHookAsync(BigQueryHook, GoogleBaseHook):
    conn_name_attr = "gcp_conn_id"
    default_conn_name = "google_cloud_bigquery_default"
    conn_type = "gcpbigquery"
    hook_name = "Google Bigquery"
    _job_conn: Job = None

    def __init__(
        self,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        delegate_to: Optional[str] = None,
        use_legacy_sql: bool = True,
        location: Optional[str] = None,
        bigquery_conn_id: Optional[str] = None,
        api_resource_configs: Optional[Dict] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        labels: Optional[Dict] = None,
    ) -> None:
        # To preserve backward compatibility
        # TODO: remove one day
        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            gcp_conn_id = bigquery_conn_id
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.running_job_id = None
        self.api_resource_configs = api_resource_configs if api_resource_configs else {}  # type Dict
        self.labels = labels

        import os

        for item, value in os.environ.items():
            print("{}: {}".format(item, value))

    @GoogleBaseHook.fallback_to_default_project_id
    def insert_job(
        self,
        configuration: Dict,
        job_id: Optional[str] = None,
        project_id: Optional[str] = None,
        location: Optional[str] = None,
    ) -> BigQueryJob:
        """
        Initiates a BigQuery job and returns a job instance containing the job id.
        Uses job._begin() instead of a blocking job.result() call, used in BigQueryHook
        See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        :param configuration: The configuration parameter maps directly to
            BigQuery's configuration field in the job object. See
            https://cloud.google.com/bigquery/docs/reference/v2/jobs for
            details.
        :type configuration: Dict[str, Any]
        :param job_id: The ID of the job. The ID must contain only letters (a-z, A-Z),
            numbers (0-9), underscores (_), or dashes (-). The maximum length is 1,024
            characters. If not provided then uuid will be generated.
        :type job_id: str
        :param project_id: Google Cloud Project where the job is running
        :type project_id: str
        :param location: location the job is running
        :type location: str
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
        # Use the _begin() method which will just initiate the job
        job._begin()
        return job

    def get_job_conn(self, project_id, job_id, s) -> Job:
        """Returns a connection to Google Cloud Job API"""
        if not self._job_conn:
            with self.provide_gcp_credential_file_as_context() as conn:
                self._job_conn = Job(job_id=job_id, project=project_id, service_file=conn, session=s)
        return self._job_conn

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
                job_client = self.get_job_conn(project_id, job_id, s)
                job_status_response = await job_client.result(s)
                if job_status_response:
                    job_status = "success"
            except OSError:
                job_status = "pending"
            except Exception as e:
                self.log.info("Query execution finished with errors...")
                job_status = str(e)
            return job_status
