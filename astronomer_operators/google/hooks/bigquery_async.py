# #
# # Licensed to the Apache Software Foundation (ASF) under one
# # or more contributor license agreements.  See the NOTICE file
# # distributed with this work for additional information
# # regarding copyright ownership.  The ASF licenses this file
# # to you under the Apache License, Version 2.0 (the
# # "License"); you may not use this file except in compliance
# # with the License.  You may obtain a copy of the License at
# #
# #   http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing,
# # software distributed under the License is distributed on an
# # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# # KIND, either express or implied.  See the License for the
# # specific language governing permissions and limitations
# # under the License.
# #
# """
# This module contains a BigQueryHookAsync
# """
import warnings
from typing import Dict, Optional, Sequence, Union

from aiohttp import ClientSession as Session
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from gcloud.aio.bigquery import Job


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
        """Polls job status asynchronously"""
        async with Session() as s:
            print("In get_job_status...")
            job_client = self.get_job_conn(project_id, job_id, s)
            print("Job API instantiated")
            print(job_client.__dict__)
            job_status_response = await job_client.get_query_results()
            print("job_status_response is", job_status_response)
