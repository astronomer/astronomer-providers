from typing import Any, AsyncIterator, Dict, List, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHookAsync


class RedshiftSQLTrigger(BaseTrigger):  # noqa: D101
    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        aws_conn_id: str,
        query_ids: List[str],
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.aws_conn_id = aws_conn_id
        self.query_ids = query_ids

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes RedshiftSQLTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.redshift_sql.RedshiftSQLTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "aws_conn_id": self.aws_conn_id,
                "query_ids": self.query_ids,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make async connection and execute query using the Amazon Redshift Data API."""
        hook = RedshiftSQLHookAsync(aws_conn_id=self.aws_conn_id)
        try:
            response = await hook.get_query_status(self.query_ids)
            if response:
                yield TriggerEvent(response)
                return
            else:
                error_message = f"{self.task_id} failed"
                yield TriggerEvent({"status": "error", "message": error_message})
                return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
