#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""This module contains a Google Cloud Storage hook."""
import warnings
from typing import Optional

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from asgiref.sync import sync_to_async
from gcloud.aio.storage import Storage

DEFAULT_TIMEOUT = 60


class GCSAsyncHook(GoogleBaseHook):
    _conn = None  # type: Optional[Storage]

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        google_cloud_storage_conn_id: Optional[str] = None,
    ) -> None:
        # To preserve backward compatibility
        # TODO: remove one day
        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            gcp_conn_id = google_cloud_storage_conn_id
        super().__init__(
            gcp_conn_id=gcp_conn_id,
        )

    async def get_storage_instance(self, session) -> Storage:
        """Returns a Google Cloud Storage service object."""
        with await sync_to_async(self.provide_gcp_credential_file_as_context)() as conn:
            self._conn = Storage(service_file=conn, session=session)
        return self._conn
