import asyncio
from typing import Dict, List, Union

import botocore.exceptions
from asgiref.sync import sync_to_async

from astronomer.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook


class RedshiftSQLHookAsync(RedshiftDataHook):
    """RedshiftSQL async hook inherits from RedshiftDataHook to interact with AWS redshift cluster database"""

    async def get_query_status(self, query_ids: List[str]) -> Dict[str, Union[str, List[str]]]:
        """
        Async function to get the Query status by query Ids, this function
        takes list of query_ids make async connection
        to redshift data to get the query status by query id returns the query status.

        :param query_ids: list of query ids
        """
        try:
            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = await sync_to_async(self.get_conn)()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = await sync_to_async(self.get_conn)()
            completed_ids: List[str] = []
            for qid in query_ids:
                while await self.is_still_running(qid):
                    await asyncio.sleep(1)
                res = client.describe_statement(Id=qid)
                if res["Status"] == "FINISHED":
                    completed_ids.append(qid)
                elif res["Status"] == "FAILED":
                    msg = "Error: " + res["QueryString"] + " query Failed due to, " + res["Error"]
                    return {"status": "error", "message": msg, "query_id": qid, "type": res["Status"]}
                elif res["Status"] == "ABORTED":
                    return {
                        "status": "error",
                        "message": "The query run was stopped by the user.",
                        "query_id": qid,
                        "type": res["Status"],
                    }
            return {"status": "success", "completed_ids": completed_ids}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}

    async def is_still_running(self, qid: str) -> Union[bool, Dict[str, str]]:
        """
        Async function to whether the query is still running or in
        "PICKED", "STARTED", "SUBMITTED" state and returns True else
        return False
        """
        try:
            try:
                # for apache-airflow-providers-amazon>=3.0.0
                client = await sync_to_async(self.get_conn)()
            except ValueError:
                # for apache-airflow-providers-amazon>=4.1.0
                self.resource_type = None
                client = await sync_to_async(self.get_conn)()
            desc = client.describe_statement(Id=qid)
            if desc["Status"] in ["PICKED", "STARTED", "SUBMITTED"]:
                return True
            return False
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}
