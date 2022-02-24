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
from typing import Any, Dict, Optional, Union

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

        :param query_results: the results from a SQL query
        :param nocast: indicates whether casting to bq data type is required or not
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

<<<<<<< HEAD
<<<<<<< HEAD
=======
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
        self.log.info("Executing get_first method...")
        job_query_response = await self.get_job_output(project_id, job_id)
        rows = job_query_response.get("rows")
        records = []
        if rows:
            records = [field.get("v") for field in rows[0].get("f")]
        return records

>>>>>>> 932df9d (Add negative test for BigQueryValueCheckOperatorAsync)
=======
>>>>>>> 7f7e216 (remove get_first_row method from hook and test parametrize)
    def value_check(
        self,
        sql: str,
        pass_value: Any,
        records: Any,
        tolerance: Any = None,
    ):
        """
        Match a single query resulting row and tolerance with pass_value
        :return: If Match fail,
            we throw an AirflowException.
        """
        if not records:
            raise AirflowException("The query returned None")
        pass_value_conv = self._convert_to_float_if_possible(pass_value)
        is_numeric_value_check = isinstance(pass_value_conv, float)
        has_tolerance = tolerance is not None
        tolerance_pct_str = str(tolerance * 100) + "%" if has_tolerance else None

        error_msg = (
            "Test failed.\nPass value:{pass_value_conv}\n"
            "Tolerance:{tolerance_pct_str}\n"
            "Query:\n{sql}\nResults:\n{records!s}"
        ).format(
            pass_value_conv=pass_value_conv,
            tolerance_pct_str=tolerance_pct_str,
            sql=sql,
            records=records,
        )

        if not is_numeric_value_check:
            tests = [str(record) == pass_value_conv for record in records]
        else:
            try:
                numeric_records = [float(record) for record in records]
            except (ValueError, TypeError):
                raise AirflowException(f"Converting a result to float failed.\n{error_msg}")
            tests = self._get_numeric_matches(numeric_records, pass_value_conv, tolerance)

        if not all(tests):
            raise AirflowException(error_msg)

    @staticmethod
    def _get_numeric_matches(records: list[float], pass_value: float, tolerance: float = None):
        """
        A helper function to match numeric pass_value, tolerance with records value

        :param records: List of value to match against
        :param pass_value: Expected value
        :param tolerance: Allowed tolerance for match to succeed
        """
        if tolerance:
            return [
                pass_value * (1 - tolerance) <= record <= pass_value * (1 + tolerance) for record in records
            ]

        return [record == pass_value for record in records]

    @staticmethod
    def _convert_to_float_if_possible(s):
        """
        A small helper function to convert a string to a numeric value
        if appropriate

        :param s: the string to be converted
        """
        try:
            ret = float(s)
        except (ValueError, TypeError):
            ret = s
        return ret
