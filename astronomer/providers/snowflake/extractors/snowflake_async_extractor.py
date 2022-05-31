from typing import Any, Dict, List, Optional

from airflow.models import Connection
from openlineage.airflow.extractors.base import BaseExtractor, TaskMetadata
from openlineage.airflow.extractors.dbapi_utils import get_table_schemas
from openlineage.airflow.utils import get_connection, get_connection_uri  # noqa
from openlineage.client.facet import ExternalQueryRunFacet, SqlJobFacet
from openlineage.common.dataset import Source
from openlineage.common.sql import DbTableMeta, SqlMeta, parse


class SnowflakeAsyncExtractor(BaseExtractor):
    """This extractor provides visibility on the metadata of a snowflake async operator"""

    source_type = "SNOWFLAKE"
    default_schema = "PUBLIC"

    def __init__(self, operator):  # type: ignore[no-untyped-def]
        super().__init__(operator)
        self.conn: "Connection" = None
        self.hook = None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        """Returns the list of operators this extractors works on."""
        return ["SnowflakeOperatorAsync"]

    def extract(self) -> TaskMetadata:
        """Extract the Metadata from the task returns the TaskMetadata class instance type"""
        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        run_facets: Dict = {}  # type: ignore[type-arg]
        job_facets = {"sql": SqlJobFacet(self.operator.sql)}

        # (1) Parse sql statement to obtain input / output tables.
        stm = "Sending SQL to parser {0}".format(self.operator.sql)
        self.log.debug(stm)
        sql_meta: Optional[SqlMeta] = parse(self.operator.sql, self.default_schema)
        metadata = "Got meta {0}".format(sql_meta)
        self.log.debug(metadata)

        if not sql_meta:
            return TaskMetadata(
                name=task_name, inputs=[], outputs=[], run_facets=run_facets, job_facets=job_facets
            )

        # (2) Get Airflow connection
        self.conn = get_connection(self._conn_id())

        # (3) Default all inputs / outputs to current connection.
        # NOTE: We'll want to look into adding support for the `database`
        # property that is used to override the one defined in the connection.
        source = Source(
            scheme="snowflake", authority=self._get_authority(), connection_url=self._get_connection_uri()
        )

        database = self.operator.database
        if not database:
            database = self._get_database()

        # (4) Map input / output tables to dataset objects with source set
        # as the current connection. We need to also fetch the schema for the
        # input tables to format the dataset name as:
        # {schema_name}.{table_name}
        inputs, outputs = get_table_schemas(
            self._get_hook(),
            source,
            database,
            self._information_schema_query(sql_meta.in_tables) if sql_meta.in_tables else None,
            self._information_schema_query(sql_meta.out_tables) if sql_meta.out_tables else None,
        )

        query_ids = self._get_query_ids()
        if len(query_ids) == 1:
            run_facets["externalQuery"] = ExternalQueryRunFacet(
                externalQueryId=query_ids[0], source=source.name
            )
        elif len(query_ids) > 1:
            warnings_msg = (
                "Found more than one query id for task {0}: {1} This might indicate that this task "
                "might be better as multiple jobs".format(task_name, query_ids)
            )
            self.log.warning(warnings_msg)

        return TaskMetadata(
            name=task_name,
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def _information_schema_query(self, tables: List[DbTableMeta]) -> str:
        """
        Forms the information execution query with table names and Returns SQL query

        :param tables: List of table names
        """
        table_names = ",".join((f"'{self._normalize_identifiers(name.name)}'" for name in tables))
        database = self.operator.database
        if not database:
            database = self._get_database()
        # fmt: off
        sql = (  # nosec
            f"SELECT table_schema, table_name, column_name, ordinal_position, "
            f"data_type FROM {database}.information_schema.columns WHERE table_name IN ({table_names});"
        )
        # fmt: on
        return sql

    def _get_database(self) -> str:
        """Get the hook information and returns the database name"""
        if hasattr(self.operator, "database") and self.operator.database is not None:
            return str(self.operator.database)
        return str(
            self.conn.extra_dejson.get("extra__snowflake__database", "")
            or self.conn.extra_dejson.get("database", "")
        )

    def _get_authority(self) -> str:
        """Get the hook information and returns the account name"""
        if hasattr(self.operator, "account") and self.operator.account is not None:
            return str(self.operator.account)
        return str(
            self.conn.extra_dejson.get("extra__snowflake__account", "")
            or self.conn.extra_dejson.get("account", "")
        )

    def _get_hook(self) -> Any:
        """
        Get the connection details from the hooks class based on the operator and returns
        hooks connection details
        """
        if hasattr(self.operator, "get_db_hook"):
            return self.operator.get_db_hook()
        else:
            return self.operator.get_hook()

    def _conn_id(self) -> Any:
        """Return the connection id from the class"""
        return self.operator.snowflake_conn_id

    def _normalize_identifiers(self, table: str) -> str:
        """
        Snowflake keeps it's table names in uppercase, so we need to normalize
        them before use: see
        https://community.snowflake.com/s/question/0D50Z00009SDHEoSAP/is-there-case-insensitivity-for-table-name-or-column-names  # noqa
        """
        return table.upper()

    def _get_connection_uri(self) -> Any:
        """Return the connection uri from the connection details by passing the connection id"""
        return get_connection_uri(self.conn)

    def _get_query_ids(self) -> List[str]:
        """Returns the list of query ids from the class"""
        if hasattr(self.operator, "query_ids"):
            return self.operator.query_ids  # type: ignore[no-any-return]
        return []
