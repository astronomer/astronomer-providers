from typing import Any, List, Optional

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.logging_mixin import LoggingMixin
from google.cloud.bigquery import Client
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import get_job_name
from openlineage.common.provider.bigquery import BigQueryDatasetsProvider

from astronomer.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperatorAsync,
)


class BigQueryAsyncExtractor(BaseExtractor, LoggingMixin):
    """
    This extractor provides visibility on the metadata of a BigQuery Insert Job
    including ``billedBytes``, ``rowCount``, ``size``, etc. submitted from a
    ``BigQueryInsertJobOperatorAsync`` operator.
    """

    def __init__(self, operator: BigQueryInsertJobOperatorAsync):
        super().__init__(operator)
        self._big_query_client = self._get_big_query_client()

    def _get_big_query_client(self) -> Client:
        """
        Gets the BigQuery client to fetch job metadata.
        The method checks whether a connection hook is available with the Airflow configuration for the operator, and
        if yes, returns the same connection. Otherwise, returns the Client instance of``google.cloud.bigquery``.
        """
        if hasattr(self.operator, "hook") and self.operator.hook:
            hook = self.operator.hook
            return hook.get_client(project_id=hook.project_id, location=hook.location)
        return Client()

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
        return ["BigQueryInsertJobOperatorAsync"]

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
        stats = BigQueryDatasetsProvider(client=self._big_query_client).get_facets(bigquery_job_id)
        inputs = stats.inputs
        output = stats.output
        run_facets = stats.run_facets

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[output.to_openlineage_dataset()] if output else [],
            run_facets=run_facets,
        )
