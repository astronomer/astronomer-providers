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
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, _bq_cast
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

    async def get_job_output(
        self,
        job_id: str,
        project_id: Optional[str] = None,
    ) -> Dict:
        """
        Get the big query job output for the given job id
        asynchronously using gcloud-aio.
        """
        async with Session() as session:
            self.log.info("Executing get_job_output..")
            job_client = await self.get_job_instance(project_id, job_id, session)
            job_query_response = await job_client.get_query_results(session)
            return job_query_response

    def get_records(self, query_results: Dict, nocast: bool = True) -> list:
        """
        Given the output query response from gcloud aio bigquery,
        convert the response to records.
        nocast: indicates whether casting to bq data type is required or not
        """
        buffer = []
        if "rows" in query_results and query_results["rows"]:
            fields = query_results["schema"]["fields"]
            col_types = [field["type"] for field in fields]
            rows = query_results["rows"]
            for dict_row in rows:
                if nocast:
                    typed_row = [vs["v"] for vs in dict_row["f"]]
                else:
                    typed_row = [_bq_cast(vs["v"], col_types[idx]) for idx, vs in enumerate(dict_row["f"])]
                buffer.append(typed_row)
        return buffer

    async def get_first_row(
        self,
        job_id: str,
        project_id: Optional[str] = None,
    ):
        """
        Get the first resulting row of a query execution job
        :param job_id: Job ID of the query job
        :type job_id: str
        :project_id: Project ID of the query job
        :type project_id: Optional[str]
        """
        async with Session() as s:
            self.log.info("Executing get_first_row method...")
            job_client = await self.get_job_instance(project_id, job_id, s)
            job_query_response = await job_client.get_query_results(s)
            rows = job_query_response.get("rows")
            records = []
            if rows:
                records = [field.get("v") for field in rows[0].get("f")]
            return records

    def interval_check(
        self, row1: str, row2: str, metrics_thresholds: dict, ignore_zero: bool, ratio_formula: str
    ):
        """
        Checks that the values of metrics given as SQL expressions are within a certain tolerance of the ones from
        days_back before.
        """
        if not row2:
            raise AirflowException(f"The query {self.sql2} returned None")
        if not row1:
            raise AirflowException(f"The query {self.sql1} returned None")

        ratio_formulas = {
            "max_over_min": lambda cur, ref: float(max(cur, ref)) / min(cur, ref),
            "relative_diff": lambda cur, ref: float(abs(cur - ref)) / ref,
        }

        metrics_sorted = sorted(metrics_thresholds.keys())

        current = dict(zip(metrics_sorted, row1))
        reference = dict(zip(metrics_sorted, row2))
        ratios = {}
        test_results = {}

        for metric in metrics_sorted:
            cur = float(current[metric])
            ref = float(reference[metric])
            threshold = float(metrics_thresholds[metric])

            if cur == 0 or ref == 0:
                ratios[metric] = None
                test_results[metric] = ignore_zero
            else:
                ratios[metric] = ratio_formulas[ratio_formula](
                    float(current[metric]), float(reference[metric])
                )
                test_results[metric] = float(ratios[metric]) < threshold

            self.log.info(
                (
                    "Current metric for %s: %s\n"
                    "Past metric for %s: %s\n"
                    "Ratio for %s: %s\n"
                    "Threshold: %s\n"
                ),
                metric,
                cur,
                metric,
                ref,
                metric,
                ratios[metric],
                threshold,
            )

        if not all(test_results.values()):
            failed_tests = [it[0] for it in test_results.items() if not it[1]]
            self.log.warning(
                "The following %s tests out of %s failed:",
                len(failed_tests),
                len(self.metrics_sorted),
            )
            for k in failed_tests:
                self.log.warning(
                    "'%s' check failed. %s is above %s",
                    k,
                    ratios[k],
                    self.metrics_thresholds[k],
                )
            raise AirflowException(f"The following tests have failed:\n {', '.join(sorted(failed_tests))}")

        self.log.info("All tests have passed")
