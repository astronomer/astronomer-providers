from typing import List

from airflow.models.taskinstance import TaskInstance
from openlineage.airflow.extractors.redshift_data_extractor import RedshiftDataExtractor

SCHEMA_URI = "https://raw.githubusercontent.com/OpenLineage/OpenLineage/main/spec/OpenLineage.json"


class RedshiftAsyncExtractor(RedshiftDataExtractor):
    """This extractor provides visibility on the metadata of a RedshiftDataOperatorAsync"""

    default_schema = "public"

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        """Returns the list of operators this extractor works on."""
        return ["RedshiftDataOperatorAsync", "RedshiftSQLOperatorAsync"]

    def _get_xcom_redshift_job_id(self, task_instance: TaskInstance) -> str:
        """Get query ids from XCOM"""
        redshift_job_id: List[str]
        redshift_job_id = task_instance.xcom_pull(task_ids=task_instance.task_id, key="return_value")
        print("redhsift job id", redshift_job_id)
        if len(redshift_job_id) > 0:
            return redshift_job_id[0]
        return None
