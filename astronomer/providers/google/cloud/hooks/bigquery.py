from typing import Any, Dict, List, Optional, Union, cast

from aiohttp import ClientSession as ClientSession
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from gcloud.aio.bigquery import Job, Table
from google.cloud.bigquery import CopyJob, ExtractJob, LoadJob, QueryJob
from requests import Session

from astronomer.providers.google.common.hooks.base_google import GoogleBaseHookAsync

try:
    from airflow.providers.google.cloud.utils.bigquery import bq_cast
except ImportError:
    # For apache-airflow-providers-google < 8.5.0
    from airflow.providers.google.cloud.hooks.bigquery import (  # type: ignore[no-redef,attr-defined] # noqa: E501 # pragma: no cover
        _bq_cast as bq_cast,
    )

BigQueryJob = Union[CopyJob, QueryJob, LoadJob, ExtractJob]


class BigQueryHookAsync(GoogleBaseHookAsync):
    """Big query async hook inherits from GoogleBaseHookAsync class and connects to the Google Big Query"""

    sync_hook_class = BigQueryHook

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    async def get_job_instance(
        self, project_id: Optional[str], job_id: Optional[str], session: ClientSession
    ) -> Job:
        """Get the specified job resource by job ID and project ID."""
        with await self.service_file_as_context() as f:
            return Job(job_id=job_id, project=project_id, service_file=f, session=cast(Session, session))

    async def get_job_status(
        self,
        job_id: Optional[str],
        project_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Polls for job status asynchronously using gcloud-aio.

        Note that an OSError is raised when Job results are still pending.
        Exception means that Job finished with errors
        """
        async with ClientSession() as s:
            try:
                self.log.info("Executing get_job_status...")
                job_client = await self.get_job_instance(project_id, job_id, s)
                job_status_response = await job_client.result(cast(Session, s))
                if job_status_response:
                    job_status = "success"
            except OSError:
                job_status = "pending"
            except Exception as e:
                self.log.info("Query execution finished with errors...")
                job_status = str(e)
            return job_status

    async def get_job_output(
        self,
        job_id: Optional[str],
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get the big query job output for the given job id asynchronously using gcloud-aio."""
        async with ClientSession() as session:
            self.log.info("Executing get_job_output..")
            job_client = await self.get_job_instance(project_id, job_id, session)
            job_query_response = await job_client.get_query_results(cast(Session, session))
            return job_query_response

    def get_records(self, query_results: Dict[str, Any]) -> List[Any]:
        """
        Given the output query response from gcloud aio bigquery, convert the response to records.

        :param query_results: the results from a SQL query
        """
        buffer = []
        if "rows" in query_results and query_results["rows"]:
            rows = query_results["rows"]
            fields = query_results["schema"]["fields"]
            col_types = [field["type"] for field in fields]
            for dict_row in rows:
                typed_row = [bq_cast(vs["v"], col_types[idx]) for idx, vs in enumerate(dict_row["f"])]
                buffer.append(typed_row)
        return buffer

    def value_check(
        self,
        sql: str,
        pass_value: Any,
        records: List[Any],
        tolerance: Optional[float] = None,
    ) -> None:
        """
        Match a single query resulting row and tolerance with pass_value

        :return: If Match fail, we throw an AirflowException.
        """
        if not records:
            raise AirflowException("The query returned None")
        pass_value_conv = self._convert_to_float_if_possible(pass_value)
        is_numeric_value_check = isinstance(pass_value_conv, float)
        tolerance_pct_str = str(tolerance * 100) + "%" if tolerance else None

        error_msg = (
            "Test failed.\nPass value:{pass_value_conv}\n"
            "Tolerance:{tolerance_pct_str}\n"
            "Query:\n{sql}\nResults:\n{records!s}"
        ).format(
            pass_value_conv=pass_value_conv,
            tolerance_pct_str=tolerance_pct_str,
            sql=sql,
            records=records,
        )

        if not is_numeric_value_check:
            tests = [str(record) == pass_value_conv for record in records]
        else:
            try:
                numeric_records = [float(record) for record in records]
            except (ValueError, TypeError):
                raise AirflowException(f"Converting a result to float failed.\n{error_msg}")
            tests = self._get_numeric_matches(numeric_records, pass_value_conv, tolerance)

        if not all(tests):
            raise AirflowException(error_msg)

    @staticmethod
    def _get_numeric_matches(
        records: List[float], pass_value: Any, tolerance: Optional[float] = None
    ) -> List[bool]:
        """
        A helper function to match numeric pass_value, tolerance with records value

        :param records: List of value to match against
        :param pass_value: Expected value
        :param tolerance: Allowed tolerance for match to succeed
        """
        if tolerance:
            return [
                pass_value * (1 - tolerance) <= record <= pass_value * (1 + tolerance) for record in records
            ]

        return [record == pass_value for record in records]

    @staticmethod
    def _convert_to_float_if_possible(s: Any) -> Any:
        """
        A small helper function to convert a string to a numeric value if appropriate

        :param s: the string to be converted
        """
        try:
            ret = float(s)
        except (ValueError, TypeError):
            ret = s
        return ret

    def interval_check(
        self,
        row1: Optional[str],
        row2: Optional[str],
        metrics_thresholds: Dict[str, Any],
        ignore_zero: bool,
        ratio_formula: str,
    ) -> None:
        """
        Checks that the values of metrics given as SQL expressions are within a certain tolerance

        :param row1: first resulting row of a query execution job for first SQL query
        :param row2: first resulting row of a query execution job for second SQL query
        :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
            example 'COUNT(*)': 1.5 would require a 50 percent or less difference
            between the current day, and the prior days_back.
        :param ignore_zero: whether we should ignore zero metrics
        :param ratio_formula: which formula to use to compute the ratio between
            the two metrics. Assuming cur is the metric of today and ref is
            the metric to today - days_back.
            max_over_min: computes max(cur, ref) / min(cur, ref)
            relative_diff: computes abs(cur-ref) / ref
        """
        if not row2:
            raise AirflowException("The second SQL query returned None")
        if not row1:
            raise AirflowException("The first SQL query returned None")

        ratio_formulas = {
            "max_over_min": lambda cur, ref: float(max(cur, ref)) / min(cur, ref),
            "relative_diff": lambda cur, ref: float(abs(cur - ref)) / ref,
        }

        metrics_sorted = sorted(metrics_thresholds.keys())

        current = dict(zip(metrics_sorted, row1))
        reference = dict(zip(metrics_sorted, row2))
        ratios: Dict[str, Any] = {}
        test_results: Dict[str, Any] = {}

        for metric in metrics_sorted:
            cur = float(current[metric])
            ref = float(reference[metric])
            threshold = float(metrics_thresholds[metric])
            if cur == 0 or ref == 0:
                ratios[metric] = None
                test_results[metric] = ignore_zero
            else:
                ratios[metric] = ratio_formulas[ratio_formula](
                    float(current[metric]), float(reference[metric])
                )
                test_results[metric] = float(ratios[metric]) < threshold

            self.log.info(
                (
                    "Current metric for %s: %s\n"
                    "Past metric for %s: %s\n"
                    "Ratio for %s: %s\n"
                    "Threshold: %s\n"
                ),
                metric,
                cur,
                metric,
                ref,
                metric,
                ratios[metric],
                threshold,
            )

        if not all(test_results.values()):
            failed_tests = [it[0] for it in test_results.items() if not it[1]]
            self.log.warning(
                "The following %s tests out of %s failed:",
                len(failed_tests),
                len(metrics_sorted),
            )
            for k in failed_tests:
                self.log.warning(
                    "'%s' check failed. %s is above %s",
                    k,
                    ratios[k],
                    metrics_thresholds[k],
                )
            raise AirflowException(f"The following tests have failed:\n {', '.join(sorted(failed_tests))}")

        self.log.info("All tests have passed")


class BigQueryTableHookAsync(GoogleBaseHookAsync):
    """Class to get async hook for Bigquery Table Async"""

    sync_hook_class = BigQueryHook

    async def get_table_client(
        self, dataset: str, table_id: str, project_id: str, session: ClientSession
    ) -> Table:
        """
        Returns a Google Big Query Table object.

        :param dataset:  The name of the dataset in which to look for the table storage bucket.
        :param table_id: The name of the table to check the existence of.
        :param project_id: The Google cloud project in which to look for the table.
            The connection supplied to the hook must provide
            access to the specified project.
        :param session: aiohttp ClientSession
        """
        with await self.service_file_as_context() as file:
            return Table(
                dataset_name=dataset,
                table_name=table_id,
                project=project_id,
                service_file=file,
                session=cast(Session, session),
            )
