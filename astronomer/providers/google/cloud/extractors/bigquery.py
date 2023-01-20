import json
from typing import Any, List, Optional, Union

import attr
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.bigquery import Client
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import get_job_name
from openlineage.client.facet import SqlJobFacet
from openlineage.common.provider.bigquery import BigQueryDatasetsProvider
from openlineage.common.sql import parse

from astronomer.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperatorAsync,
    BigQueryGetDataOperatorAsync,
    BigQueryInsertJobOperatorAsync,
    BigQueryIntervalCheckOperatorAsync,
    BigQueryValueCheckOperatorAsync,
)


@attr.s
class SqlContext:
    """Internal SQL context for holding query parser results"""

    sql: str = attr.ib()
    inputs: Optional[str] = attr.ib(default=None)
    outputs: Optional[str] = attr.ib(default=None)
    parser_error: Optional[str] = attr.ib(default=None)


class BigQueryAsyncExtractor(BaseExtractor):
    """
    This extractor provides visibility on the metadata of a BigQuery Insert Job
    including ``billedBytes``, ``rowCount``, ``size``, etc. submitted from a
    ``BigQueryInsertJobOperatorAsync`` operator.
    """

    def __init__(
        self,
        operator: Union[
            BigQueryCheckOperatorAsync,
            BigQueryGetDataOperatorAsync,
            BigQueryInsertJobOperatorAsync,
            BigQueryIntervalCheckOperatorAsync,
            BigQueryValueCheckOperatorAsync,
        ],
    ):
        super().__init__(operator)

    def _get_big_query_client(self) -> Client:
        """
        Gets the BigQuery client to fetch job metadata.
        The method checks whether a connection hook is available with the Airflow configuration for the operator, and
        if yes, returns the same connection. Otherwise, returns the Client instance of``google.cloud.bigquery``.
        """
        hook = BigQueryHook(gcp_conn_id=self.operator.gcp_conn_id)
        return hook.get_client(project_id=hook.project_id, location=hook.location)

    def _get_xcom_bigquery_job_id(self, task_instance: TaskInstance) -> Any:
        """
        Pulls the BigQuery Job ID from XCOM for the task instance whose metadata needs to be extracted.

        :param task_instance: Instance of the Airflow task whose BigQuery ``job_id`` needs to be pulled from XCOM.
        """
        bigquery_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key="job_id")
        if not bigquery_job_id:
            raise AirflowException("Could not pull relevant BigQuery job ID from XCOM")
        self.log.debug("Big Query Job Id %s", bigquery_job_id)
        return bigquery_job_id

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        """Returns the list of operators this extractor works on."""
        return [
            "BigQueryCheckOperatorAsync",
            "BigQueryGetDataOperatorAsync",
            "BigQueryInsertJobOperatorAsync",
            "BigQueryIntervalCheckOperatorAsync",
            "BigQueryValueCheckOperatorAsync",
        ]

    def extract(self) -> Optional[TaskMetadata]:
        """Empty extract implementation for the abstractmethod of the ``BaseExtractor`` class."""
        return None

    def extract_on_complete(self, task_instance: TaskInstance) -> Optional[TaskMetadata]:
        """
        Callback on task completion to fetch metadata extraction details that are to be pushed to the Lineage server.

        :param task_instance: Instance of the Airflow task whose metadata needs to be extracted.
        """
        try:
            bigquery_job_id = self._get_xcom_bigquery_job_id(task_instance)
        except AirflowException as ae:
            exception_message = str(ae)
            self.log.exception("%s", exception_message)
            return TaskMetadata(name=get_job_name(task=self.operator))

        # We want to use the operator hook's client to fetch metadata details from remote Google cloud services.
        # The hook is attached to the operator during its initialization for execution. Hence, to reuse the hook's
        # client we want to delay referencing of the client up until here and not do it in the constructor itself
        # which would be called while the operator is still executing and the hook might not have been attached yet.
        self._big_query_client = self._get_big_query_client()

        stats = BigQueryDatasetsProvider(client=self._big_query_client).get_facets(bigquery_job_id)
        inputs = stats.inputs
        output = stats.output
        run_facets = stats.run_facets
        job_facets = {}
        if isinstance(
            self.operator,
            (BigQueryCheckOperatorAsync, BigQueryInsertJobOperatorAsync, BigQueryValueCheckOperatorAsync),
        ):
            context = self.parse_sql_context()
            job_facets["sql"] = SqlJobFacet(context.sql)

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[output.to_openlineage_dataset()] if output else [],
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def parse_sql_context(self) -> SqlContext:
        """Gets SQL from operator and transforms it to `SqlContext` object."""
        if isinstance(self.operator, BigQueryInsertJobOperatorAsync):
            self.operator.sql = self.operator.configuration["query"]["query"]
        sql_meta = parse(self.operator.sql, dialect="bigquery")
        return SqlContext(
            sql=self.operator.sql,
            inputs=json.dumps([in_table.name for in_table in sql_meta.in_tables]),
            outputs=json.dumps([out_table.name for out_table in sql_meta.out_tables]),
        )
