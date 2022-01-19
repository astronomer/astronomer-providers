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


"""This module contains Google BigQueryAsync operators."""
import enum
import os
import warnings
from typing import TYPE_CHECKING, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from astronomer_operators.google.hooks.bigquery_async import BigQueryHookAsync
from astronomer_operators.google.triggers.bigquery_async import BigQueryTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context

BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"

_DEPRECATION_MSG = (
    "The bigquery_conn_id parameter has been deprecated. You should pass the gcp_conn_id parameter."
)


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators"""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"


class BigQueryGetDataOperatorAsync(BaseOperator):
    template_fields: Sequence[str] = (
        "dataset_id",
        "table_id",
        "max_results",
        "selected_fields",
        "impersonation_chain",
    )
    ui_color = BigQueryUIColors.QUERY.value

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        max_results: int = 100,
        selected_fields: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        print("In __init method of BigQueryGetDataOperatorAsync")
        print(
            "dataset_id",
            dataset_id,
            "table_id",
            table_id,
            "max_results",
            max_results,
            "selected_fields",
            selected_fields,
            "gcp_conn_id",
            gcp_conn_id,
            "bigquery_conn_id",
            bigquery_conn_id,
            "delegate_to",
            delegate_to,
            "location",
            location,
            "impersonation_chain",
            impersonation_chain,
        )

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.table_id = table_id
        self.max_results = int(max_results)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.project_id = os.environ.get("GCP_PROJECT_ID", None)

    def execute(self, context: "Context"):
        print("In execute method")
        self.log.info(
            "Fetching Data from %s.%s max results: %s", self.dataset_id, self.table_id, self.max_results
        )

        hook = BigQueryHookAsync(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        configuration = {
            "query": {
                "query": f"SELECT * FROM {self.dataset_id}.{self.table_id}",
            }
        }

        job_response = hook.insert_job(configuration=configuration, project_id=self.project_id)
        running_job_id = job_response.job_id
        print(f"The running job id is {running_job_id}")

        print("executing defer from the BigQueryGetDataOperatorAsync")

        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryTrigger(
                conn_id=self.gcp_conn_id,
                job_id=running_job_id,
                dataset_id=self.dataset_id,
                project_id=self.project_id,
                table_id=self.table_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        print("In execute_complete")
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed successfully with response %s ",
            self.task_id,
            event["api_response"],
        )
        return event["api_response"]
