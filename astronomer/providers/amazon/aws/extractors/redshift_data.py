import logging
import traceback
from typing import Any, Dict, List, Optional

import attr
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.utils import get_job_name
from openlineage.client.facet import (
    BaseFacet,
    OutputStatisticsOutputDatasetFacet,
    SqlJobFacet,
)
from openlineage.common.dataset import Dataset, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta, SqlMeta, parse

SCHEMA_URI = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json"


class RedshiftAsyncDataExtractor(BaseExtractor):
    """This extractor provides visibility on the metadata of a RedshiftDataOperatorAsync"""

    default_schema = "public"

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        """Returns the list of operators this extractor works on."""
        return ["RedshiftDataOperatorAsync"]

    def extract(self) -> Optional[TaskMetadata]:
        """Empty extract implementation for the abstractmethod of the ``BaseExtractor`` class."""
        return None

    def extract_on_complete(self, task_instance) -> Optional[TaskMetadata]:
        """
        Callback on task completion to fetch metadata extraction details that are to be pushed to the Lineage server.

        :param task_instance: Instance of the Airflow task whose metadata needs to be extracted.
        """
        self.log.debug("extract_on_complete(%s)", task_instance)
        job_facets = {"sql": SqlJobFacet(self.operator.sql)}

        self.log.debug("Sending SQL to parser: %s", self.operator.sql)
        sql_meta: Optional[SqlMeta] = parse(self.operator.sql, self.default_schema)
        self.log.debug("Got meta %s", sql_meta)
        try:
            redshift_job_id = self._get_xcom_redshift_job_id(task_instance)
            if redshift_job_id is None:
                raise Exception("Xcom could not resolve Redshift job id. Job may have failed.")
        except Exception:
            return TaskMetadata(
                name=get_job_name(task=self.operator),
                run_facets={},
                job_facets=job_facets,
            )

        client = self.operator.hook.conn

        redshift_details = [
            "database",
            "cluster_identifier",
            "db_user",
            "secret_arn",
            "region",
        ]

        connection_details = {detail: getattr(self.operator, detail) for detail in redshift_details}

        stats = RedshiftDataDatasetsProvider(client=client, connection_details=connection_details).get_facets(
            job_id=redshift_job_id[0],
            inputs=sql_meta.in_tables if sql_meta else [],
            outputs=sql_meta.out_tables if sql_meta else [],
        )

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[ds.to_openlineage_dataset() for ds in stats.inputs],
            outputs=[ds.to_openlineage_dataset() for ds in stats.output],
            run_facets=stats.run_facets,
            job_facets={"sql": SqlJobFacet(self.operator.sql)},
        )

    def _get_xcom_redshift_job_id(self, task_instance):
        """Get query id from XCOM"""
        redshift_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key="query_ids")

        self.log.debug("redshift job id: %s", redshift_job_id)
        return redshift_job_id


# To be removed once redshift extractors are released in openlineage
@attr.s
class ErrorMessageRunFacet(BaseFacet):
    """This facet represents an error message that was the result of a job run"""

    message: str = attr.ib()
    programmingLanguage: str = attr.ib()
    stackTrace: Optional[str] = attr.ib(default=None)

    @staticmethod
    def _get_schema() -> str:
        return SCHEMA_URI + "#/definitions/ErrorMessageRunFacet"


# To be removed once redshift extractors are released in openlineage
@attr.s
class RedshiftFacets:
    """This facet represents a RedshiftFacets"""

    run_facets: Dict[str, BaseFacet] = attr.ib()
    inputs: List[Dataset] = attr.ib()
    output: List[Dataset] = attr.ib()


# To be removed once redshift extractors are released in openlineage
class RedshiftDataDatasetsProvider:
    """RedshiftDataDatasetsProvider"""

    from botocore import client

    def __init__(
        self,
        client: client,
        connection_details: Dict[str, Any],
        logger: Optional[logging.Logger] = None,
    ):
        if logger is None:
            self.logger: logging.Logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.connection_details = connection_details
        self.client = client

    def get_facets(
        self, job_id: str, inputs: List[DbTableMeta], outputs: List[DbTableMeta]
    ) -> RedshiftFacets:
        """Gets all the facets needed for redshift"""
        ds_inputs = []
        ds_outputs = []
        run_facets = {}
        dataset_stat_facet = None

        source = Source(
            scheme="redshift",
            authority=self._get_authority(),
        )
        try:
            properties = self.client.describe_statement(Id=job_id)
            dataset_stat_facet = self._get_output_statistics(properties)
        except Exception as e:
            run_facets.update(
                {
                    "errorMessage": ErrorMessageRunFacet(
                        message=f"Cannot retrieve job details from Redshift Data client. {e}",
                        programmingLanguage="PYTHON",
                        stackTrace=f"{e}: {traceback.format_exc()}",
                    )
                }
            )

        ds_inputs = self._get_dataset_from_tables(inputs, source)
        ds_outputs = self._get_dataset_from_tables(outputs, source)

        for ds_output in ds_outputs:
            ds_output.custom_facets.update({"stats": dataset_stat_facet})

        return RedshiftFacets(run_facets, ds_inputs, ds_outputs)

    def _get_output_statistics(self, properties) -> OutputStatisticsOutputDatasetFacet:
        return OutputStatisticsOutputDatasetFacet(
            rowCount=properties.get("ResultRows"),
            size=properties.get("ResultSize"),
        )

    def _get_dataset_from_tables(self, tables: List[DbTableMeta], source: Source) -> Dataset:
        try:
            return [
                Dataset.from_table_schema(
                    source=source,
                    table_schema=table_schema,
                    database_name=self.connection_details.get("database"),
                )
                for table_schema in self._get_table_schemas(tables)
            ]
        except Exception:
            return [Dataset.from_table(source, table.name) for table in tables]

    def _get_authority(self) -> str:
        return (
            f"{self.connection_details['cluster_identifier']}."
            f"{self.connection_details.get('region')}:5439"
        )

    def _get_table_safely(self, output_table_name):
        try:
            return self._get_table(output_table_name)
        except Exception:
            return None

    def _get_table_schemas(self, tables: List[DbTableMeta]) -> List[DbTableSchema]:
        if not tables:
            return []
        return [self._get_table(table) for table in tables]

    def _get_table(self, table: DbTableMeta) -> Optional[DbTableSchema]:
        kwargs: Dict[str, Any] = {
            "ClusterIdentifier": self.connection_details.get("cluster_identifier"),
            "Database": table.database or self.connection_details.get("database"),
            "Table": table.name,
            "DbUser": self.connection_details.get("db_user"),
            "SecretArn": self.connection_details.get("secret_arn"),
            "Schema": table.schema,
        }
        filter_values = {key: val for key, val in kwargs.items() if val is not None}
        redshift_table = self.client.describe_table(**filter_values)
        fields = redshift_table.get("ColumnList")
        if not fields:
            return None
        schema_name = fields[0]["schemaName"]
        columns = [
            DbColumn(
                name=fields[i].get("name"),
                type=fields[i].get("typeName"),
                ordinal_position=i,
            )
            for i in range(len(fields))
        ]

        return DbTableSchema(
            schema_name=schema_name,
            table_name=DbTableMeta(redshift_table.get("TableName")),
            columns=columns,
        )
