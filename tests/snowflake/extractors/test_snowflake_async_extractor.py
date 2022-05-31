from typing import Dict
from unittest import mock

import pytest
from airflow import DAG
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow.version import version as AIRFLOW_VERSION
from openlineage.airflow.extractors.base import TaskMetadata
from openlineage.client.facet import SqlJobFacet
from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta
from pkg_resources import parse_version

from astronomer.providers.snowflake.extractors.snowflake_async_extractor import (
    SnowflakeAsyncExtractor,
)
from astronomer.providers.snowflake.operators.snowflake import SnowflakeOperatorAsync

CONN_ID = "food_delivery_db"
CONN_URI = "snowflake://snowflake.example/db-schema?account=test_account&database=FOOD_DELIVERY&region=us-east&warehouse=snow-warehouse"  # noqa
CONN_URI_URIPARSED = "snowflake://snowflake.example/db-schema?account=%5B%27test_account%27%5D&database=%5B%27FOOD_DELIVERY%27%5D&region=%5B%27us-east%27%5D&warehouse=%5B%27snow-warehouse%27%5D"  # noqa

DB_NAME = "FOOD_DELIVERY"
DB_SCHEMA_NAME = "PUBLIC"
DB_TABLE_NAME = DbTableMeta("DISCOUNTS")
DB_TABLE_COLUMNS = [
    DbColumn(name="ID", type="int4", ordinal_position=1),
    DbColumn(name="AMOUNT_OFF", type="int4", ordinal_position=2),
    DbColumn(name="CUSTOMER_EMAIL", type="varchar", ordinal_position=3),
    DbColumn(name="STARTS_ON", type="timestamp", ordinal_position=4),
    DbColumn(name="ENDS_ON", type="timestamp", ordinal_position=5),
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME, table_name=DB_TABLE_NAME, columns=DB_TABLE_COLUMNS
)
NO_DB_TABLE_SCHEMA = []

SQL = f"SELECT * FROM {DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name};"

DAG_ID = "email_discounts"
DAG_OWNER = "datascience"
DAG_DEFAULT_ARGS = {
    "owner": DAG_OWNER,
    "depends_on_past": False,
    "start_date": days_ago(7),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["datascience@example.com"],
}
DAG_DESCRIPTION = "Email discounts to customers that have experienced order delays daily"

DAG = dag = DAG(
    DAG_ID, schedule_interval="@weekly", default_args=DAG_DEFAULT_ARGS, description=DAG_DESCRIPTION
)

TASK_ID = "select"
TASK = SnowflakeOperatorAsync(
    task_id=TASK_ID,
    snowflake_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG,
)


@mock.patch("astronomer.providers.snowflake.extractors.snowflake_async_extractor.get_table_schemas")  # noqa
@mock.patch("astronomer.providers.snowflake.extractors.snowflake_async_extractor.get_connection")
def test_extract(get_connection, mock_get_table_schemas):
    source = Source(scheme="snowflake", authority="test_account", connection_url=CONN_URI_URIPARSED)

    mock_get_table_schemas.return_value = (
        [Dataset.from_table_schema(source, DB_TABLE_SCHEMA, DB_NAME)],
        [],
    )

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    TASK.get_hook = mock.MagicMock()
    TASK.get_hook.return_value._get_conn_params.return_value = {
        "account": "test_account",
        "database": DB_NAME,
    }

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=source,
            fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS],
        ).to_openlineage_dataset()
    ]

    task_metadata = SnowflakeAsyncExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


@pytest.mark.skipif(parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"), reason="Airflow 2+ test")  # noqa
@mock.patch("astronomer.providers.snowflake.extractors.snowflake_async_extractor.get_table_schemas")  # noqa
@mock.patch("astronomer.providers.snowflake.extractors.snowflake_async_extractor.get_connection")
def test_extract_query_id(get_connection, mock_get_table_schemas):
    mock_get_table_schemas.return_value = (
        [],
        [],
    )

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    TASK.get_hook = mock.MagicMock()
    TASK.get_hook.return_value._get_conn_params.return_value = {
        "account": "test_account",
        "database": DB_NAME,
    }
    TASK.query_ids = ["1500100900"]

    task_metadata = SnowflakeAsyncExtractor(TASK).extract()

    assert task_metadata.run_facets["externalQuery"].externalQueryId == "1500100900"


@pytest.mark.skipif(parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"), reason="Airflow 2+ test")  # noqa
@mock.patch("astronomer.providers.snowflake.extractors.snowflake_async_extractor.get_table_schemas")  # noqa
@mock.patch("astronomer.providers.snowflake.extractors.snowflake_async_extractor.get_connection")
def test_extract_query_ids(get_connection, mock_get_table_schemas):
    mock_get_table_schemas.return_value = (
        [],
        [],
    )

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    TASK.get_hook = mock.MagicMock()
    TASK.get_hook.return_value._get_conn_params.return_value = {
        "account": "test_account",
        "database": DB_NAME,
    }
    TASK.query_ids = ["1500100900", "1500100911"]

    task_metadata = SnowflakeAsyncExtractor(TASK).extract()

    assert task_metadata.run_facets == {}


def test_extract_without_sql():
    async_task_ = SnowflakeOperatorAsync(
        task_id="test_1",
        snowflake_conn_id=CONN_ID,
        sql=None,
        dag=DAG,
    )
    run_facets: Dict = {}
    job_facets = {"sql": SqlJobFacet(None)}
    task_metadata = SnowflakeAsyncExtractor(async_task_).extract()
    assert task_metadata == TaskMetadata(
        name="email_discounts.test_1", inputs=[], outputs=[], run_facets=run_facets, job_facets=job_facets
    )


def test_get_operator_classnames():
    extractor = SnowflakeAsyncExtractor(TASK)
    class_name = extractor.get_operator_classnames()
    assert class_name == ["SnowflakeOperatorAsync"]


def test_get_database():
    TASK.database = DB_NAME
    task_metadata = SnowflakeAsyncExtractor(TASK)
    response = task_metadata._get_database()
    assert response == DB_NAME


def test_get_authority():
    TASK.account = "test_account"
    TASK.get_hook = mock.MagicMock()
    TASK.get_hook.return_value._get_conn_params.return_value = {
        "account": "test_account",
        "database": DB_NAME,
    }
    task_metadata = SnowflakeAsyncExtractor(TASK)
    response = task_metadata._get_authority()
    assert response == "test_account"


def test_get_query_ids():
    delattr(TASK, "query_ids")
    task_metadata = SnowflakeAsyncExtractor(TASK)
    response = task_metadata._get_query_ids()
    assert response == []
