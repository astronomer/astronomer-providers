from unittest import mock

from astronomer.providers.amazon.aws.extractors.redshift import RedshiftAsyncExtractor
from astronomer.providers.amazon.aws.operators.redshift_data import (
    RedshiftDataOperatorAsync,
)


class TestRedshiftAsyncExtractor:
    REDSHIFT_QUERY = """
    CREATE TABLE IF NOT EXISTS fruit (
                fruit_id INTEGER,
                name VARCHAR NOT NULL,
                color VARCHAR NOT NULL
                );
                """

    TASK = RedshiftDataOperatorAsync(
        task_id="select",
        sql=REDSHIFT_QUERY,
        database="dev",
    )

    def test_get_operator_classnames(self):
        extractor = RedshiftAsyncExtractor(self.TASK)
        class_name = extractor.get_operator_classnames()
        assert class_name == ["RedshiftDataOperatorAsync", "RedshiftSQLOperatorAsync"]

    @mock.patch("airflow.models.taskinstance.TaskInstance.xcom_pull")
    def test__get_xcom_redshift_job_id_none(self, mock_xcom_pull):
        mock_xcom_pull.return_value = {}
        extractor = RedshiftAsyncExtractor(self.TASK)
        redshift_job_id = extractor._get_xcom_redshift_job_id(mock_xcom_pull)
        assert redshift_job_id is None

    @mock.patch("airflow.models.taskinstance.TaskInstance")
    def test__get_xcom_redshift_job_id(self, mock_task_instance):
        mock_task_instance.xcom_pull.return_value = ["123"]
        extractor = RedshiftAsyncExtractor(self.TASK)
        redshift_job_id = extractor._get_xcom_redshift_job_id(mock_task_instance)
        assert "123" == redshift_job_id
