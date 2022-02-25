import logging
from typing import List

import botocore.exceptions
from asgiref.sync import sync_to_async
from async_timeout import asyncio

from astronomer.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook

log = logging.getLogger(__name__)


class RedshiftSQLHookAsync(RedshiftDataHook):
    async def get_query_status(self, query_ids: List[str]):
        """
        Async function to get the Query status by query Ids, this function
        takes list of query_ids make async connection
        to redshift data to get the query status by query id returns the query status.

        :param sql: list of query ids
        """
        try:
            client = await sync_to_async(self.get_conn)()
            completed_ids: List[str] = []
            for id in query_ids:
                while await self.is_still_running(id):
                    await asyncio.sleep(1)
                res = client.describe_statement(Id=id)
                if res["Status"] == "FINISHED":
                    completed_ids.append(id)
                elif res["Status"] == "FAILED":
                    msg = "Error: " + res["QueryString"] + " query Failed due to, " + res["Error"]
                    return {"status": "error", "message": msg, "query_id": id, "type": res["Status"]}
                elif res["Status"] == "ABORTED":
                    return {
                        "status": "error",
                        "message": "The query run was stopped by the user.",
                        "query_id": id,
                        "type": res["Status"],
                    }
            return {"status": "success", "completed_ids": completed_ids}
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}

    async def is_still_running(self, id: str):
        """
        Async function to whether the query is still running or in
        "PICKED", "STARTED", "SUBMITTED" state and returns True else
        return False
        """
        try:
            client = await sync_to_async(self.get_conn)()
            desc = client.describe_statement(Id=id)
            if desc["Status"] in ["PICKED", "STARTED", "SUBMITTED"]:
                return True
            return False
        except botocore.exceptions.ClientError as error:
            return {"status": "error", "message": str(error), "type": "ERROR"}
