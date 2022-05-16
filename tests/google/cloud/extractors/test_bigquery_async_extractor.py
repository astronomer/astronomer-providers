import json
from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import TaskDeferred
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from openlineage.client.facet import OutputStatisticsOutputDatasetFacet
from openlineage.common.dataset import Dataset, Source
from openlineage.common.provider.bigquery import (
    BigQueryFacets,
    BigQueryJobRunFacet,
    BigQueryStatisticsDatasetFacet,
)

from astronomer.providers.google.cloud.extractors.bigquery_async_extractor import (
    BigQueryAsyncExtractor,
)
from astronomer.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperatorAsync,
)

TEST_DATASET_LOCATION = "EU"
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET = "test-dataset"
TEST_TABLE = "test-table"
EXECUTION_DATE = datetime(2022, 1, 1, 0, 0, 0)
INSERT_DATE = EXECUTION_DATE.strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {TEST_DATASET}.{TEST_TABLE} VALUES "
    f"(42, 'monthy python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)

INPUT_STATS = [
    Dataset(
        source=Source(scheme="bigquery"),
        name=f"astronomer-airflow-providers.{TEST_DATASET}.{TEST_TABLE}",
        fields=[],
        custom_facets={},
        input_facets={},
        output_facets={},
    )
]

OUTPUT_STATS = Dataset(
    source=Source(scheme="bigquery"),
    name=f"astronomer-airflow-providers.{TEST_DATASET}.{TEST_TABLE}",
    fields=[],
    custom_facets={"stats": BigQueryStatisticsDatasetFacet(rowCount=2, size=0)},
    input_facets={},
    output_facets={"outputStatistics": OutputStatisticsOutputDatasetFacet(rowCount=2, size=0)},
)

with open("tests/google/cloud/extractors/job_details.json") as jd_json:
    JOB_PROPERTIES = json.load(jd_json)

RUN_FACETS = {
    "bigQuery_job": BigQueryJobRunFacet(billedBytes=0, cached=False, properties=json.dumps(JOB_PROPERTIES))
}


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("astronomer.providers.google.cloud.extractors.bigquery_async_extractor._BigQueryHook")
@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
@mock.patch("airflow.models.TaskInstance.xcom_pull")
@mock.patch("openlineage.common.provider.bigquery.BigQueryDatasetsProvider.get_facets")
def test_extract_on_complete(mock_bg_dataset_provider, mock_xcom_pull, mock_hook, mock_extractor_hook):
    """
    Tests that  the custom extractor's implementation for the BigQueryInsertJobOperatorAsync is able to process the
    operator's metadata that needs to be extracted as per OpenLineage.
    """
    configuration = {
        "query": {
            "query": INSERT_ROWS_QUERY,
            "useLegacySql": False,
        }
    }
    job_id = "123456"
    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=job_id, error_result=False)
    mock_extractor_hook.return_value.insert_job.return_value = MagicMock(job_id=job_id, error_result=False)
    mock_bg_dataset_provider.return_value = BigQueryFacets(
        run_facets=RUN_FACETS, inputs=INPUT_STATS, output=OUTPUT_STATS
    )

    task_id = "insert_query_job"
    operator = BigQueryInsertJobOperatorAsync(
        task_id=task_id,
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )

    task_instance = TaskInstance(task=operator)
    with pytest.raises(TaskDeferred):
        operator.execute(context)

    bq_extractor = BigQueryAsyncExtractor(operator)
    task_meta_extract = bq_extractor.extract()
    assert task_meta_extract is None

    task_meta = bq_extractor.extract_on_complete(task_instance)

    mock_xcom_pull.assert_called_once_with(task_ids=task_instance.task_id, key="job_id")

    assert task_meta.name == f"adhoc_airflow.{task_id}"

    assert task_meta.inputs[0].facets["dataSource"].name == INPUT_STATS[0].source.scheme
    assert task_meta.inputs[0].name == INPUT_STATS[0].name

    assert task_meta.outputs[0].name == OUTPUT_STATS.name
    assert task_meta.outputs[0].facets["stats"].rowCount == 2
    assert task_meta.outputs[0].facets["stats"].size == 0

    assert task_meta.run_facets["bigQuery_job"].billedBytes == 0
    run_facet_properties = json.loads(task_meta.run_facets["bigQuery_job"].properties)
    assert run_facet_properties == JOB_PROPERTIES


def test_extractor_works_on_operator():
    """Tests that the custom extractor implementation is available for the BigQueryInsertJobOperatorAsync Operator."""
    task_id = "insert_query_job"
    operator = BigQueryInsertJobOperatorAsync(task_id=task_id, configuration={})
    assert type(operator).__name__ in BigQueryAsyncExtractor.get_operator_classnames()


@mock.patch("astronomer.providers.google.cloud.operators.bigquery._BigQueryHook")
def test_unavailable_xcom_raises_exception(mock_hook):
    """
    Tests that an exception is raised when the custom extractor is not available to retrieve required XCOM for the
    BigQueryInsertJobOperatorAsync Operator.
    """
    configuration = {
        "query": {
            "query": INSERT_ROWS_QUERY,
            "useLegacySql": False,
        }
    }
    job_id = "123456"
    mock_hook.return_value.insert_job.return_value = MagicMock(job_id=job_id, error_result=False)
    task_id = "insert_query_job"
    operator = BigQueryInsertJobOperatorAsync(
        task_id=task_id,
        configuration=configuration,
        location=TEST_DATASET_LOCATION,
        job_id=job_id,
        project_id=TEST_GCP_PROJECT_ID,
    )

    task_instance = TaskInstance(task=operator)
    execution_date = datetime(2022, 1, 1, 0, 0, 0)
    task_instance.run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)

    with pytest.raises(TaskDeferred):
        operator.execute(context)
    bq_extractor = BigQueryAsyncExtractor(operator)
    with mock.patch.object(bq_extractor.log, "exception") as mock_log_exception:
        task_meta = bq_extractor.extract_on_complete(task_instance)

    mock_log_exception.assert_called_with("%s", "Could not pull relevant BigQuery job ID from XCOM")
    assert task_meta.name == f"adhoc_airflow.{task_id}"
