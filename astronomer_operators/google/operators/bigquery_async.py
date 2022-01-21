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
import hashlib
import json
import re
import uuid
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Set, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryJob
from google.api_core.exceptions import Conflict

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


class BigQueryInsertJobOperatorAsync(BaseOperator):
    """
    Starts a BigQuery job asynchronously, and returns job id.
    This operator works in the following way:

    - it calculates a unique hash of the job using job's configuration or uuid if ``force_rerun`` is True
    - creates ``job_id`` in form of
        ``[provided_job_id | airflow_{dag_id}_{task_id}_{exec_date}]_{uniqueness_suffix}``
    - submits a BigQuery job using the ``job_id``
    - if job with given id already exists then it tries to reattach to the job if its not done and its
        state is in ``reattach_states``. If the job is done the operator will raise ``AirflowException``.

    Using ``force_rerun`` will submit a new job every time without attaching to already existing ones.

    For job definition see here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryInsertJobOperatorAsync`


    :param configuration: The configuration parameter maps directly to BigQuery's
        configuration field in the job  object. For more details see
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    :type configuration: Dict[str, Any]
    :param job_id: The ID of the job. It will be suffixed with hash of job configuration
        unless ``force_rerun`` is True.
        The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
        dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
        be generated.
    :type job_id: str
    :param force_rerun: If True then operator will use hash of uuid as job id suffix
    :type force_rerun: bool
    :param reattach_states: Set of BigQuery job's states in case of which we should reattach
        to the job. Should be other than final states.
    :param project_id: Google Cloud Project where the job is running
    :type project_id: str
    :param location: location the job is running
    :type location: str
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    :type cancel_on_kill: bool
    """

    template_fields: Sequence[str] = (
        "configuration",
        "job_id",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".json",)
    template_fields_renderers = {"configuration": "json", "configuration.query.query": "sql"}
    ui_color = BigQueryUIColors.QUERY.value

    def __init__(
        self,
        configuration: Dict[str, Any],
        project_id: Optional[str] = None,
        location: Optional[str] = None,
        job_id: Optional[str] = None,
        force_rerun: bool = True,
        reattach_states: Optional[Set[str]] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration
        self.location = location
        self.job_id = job_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.force_rerun = force_rerun
        self.reattach_states: Set[str] = reattach_states or set()
        self.impersonation_chain = impersonation_chain
        self.cancel_on_kill = cancel_on_kill
        self.hook: Optional[BigQueryHookAsync] = None

    def prepare_template(self) -> None:
        # If .json is passed then we have to read the file
        if isinstance(self.configuration, str) and self.configuration.endswith(".json"):
            with open(self.configuration) as file:
                self.configuration = json.loads(file.read())

    def _submit_job(
        self,
        hook: BigQueryHookAsync,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        return hook.insert_job(
            configuration=self.configuration,
            project_id=self.project_id,
            location=self.location,
            job_id=job_id,
        )

    @staticmethod
    def _handle_job_error(job: BigQueryJob) -> None:
        if job.error_result:
            raise AirflowException(f"BigQuery job {job.job_id} failed: {job.error_result}")

    def _job_id(self, context):
        if self.force_rerun:
            hash_base = str(uuid.uuid4())
        else:
            hash_base = json.dumps(self.configuration, sort_keys=True)

        uniqueness_suffix = hashlib.md5(hash_base.encode()).hexdigest()

        if self.job_id:
            return f"{self.job_id}_{uniqueness_suffix}"

        exec_date = context["execution_date"].isoformat()
        job_id = f"airflow_{self.dag_id}_{self.task_id}_{exec_date}_{uniqueness_suffix}"
        return re.sub(r"[:\-+.]", "_", job_id)

    def execute(self, context: "Context"):
        hook = BigQueryHookAsync(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        self.hook = hook
        job_id = self._job_id(context)

        try:
            job = self._submit_job(hook, job_id)
            self._handle_job_error(job)
        except Conflict:
            # If the job already exists retrieve it
            job = hook.get_job(
                project_id=self.project_id,
                location=self.location,
                job_id=job_id,
            )
            if job.state in self.reattach_states:
                # We are reattaching to a job
                job._begin()
                self._handle_job_error(job)
            else:
                # Same job configuration so we need force_rerun
                raise AirflowException(
                    f"Job with id: {job_id} already exists and is in {job.state} state. If you "
                    f"want to force rerun it consider setting `force_rerun=True`."
                    f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                )

        self.job_id = job.job_id

        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryTrigger(
                conn_id=self.gcp_conn_id,
                job_id=self.job_id,
                project_id=self.project_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
        return event["message"]
