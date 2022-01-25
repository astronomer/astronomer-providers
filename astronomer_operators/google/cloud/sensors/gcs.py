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
"""This module contains Google Cloud Storage sensors."""

from asyncore import poll
import os
import warnings
from datetime import datetime
from typing import Callable, List, Optional, Sequence, Set, Union

from airflow.exceptions import AirflowException
from astronomer_operators.google.cloud.hooks.gcs import GCSAsyncHook
from astronomer_operators.google.cloud.triggers.gcs import GCSTrigger
from airflow.sensors.base import BaseSensorOperator


class GCSAsyncObjectExistenceSensor(BaseSensorOperator):
    # """
    # Checks for the existence of a file in Google Cloud Storage.
    # :param bucket: The Google Cloud Storage bucket where the object is.
    # :type bucket: str
    # :param object: The name of the object to check in the Google cloud
    #     storage bucket.
    # :type object: str
    # :param google_cloud_conn_id: The connection ID to use when
    #     connecting to Google Cloud Storage.
    # :type google_cloud_conn_id: str
    # :param bucket: The bucket name where the objects in GCS will be present
    # :type bucket: str
    # :param object: the object name of the file or folder present in the google
    #       cloud storage
    # :type object: str
    # """

    template_fields = (
        'bucket',
        'object',
        'google_cloud_conn_id'
    )
    ui_color = '#f0eee4'

    def __init__(
        self,
        *,
        bucket: str,
        object: str,
        polling_interval:float = 5.0,
        google_cloud_conn_id: str = 'google_cloud_default',
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.bucket = bucket
        self.object = object
        self.polling_interval = polling_interval
        self.google_cloud_conn_id = google_cloud_conn_id

    """
    Waits for a file or folder to land in a google cloud storage using async.
    If the path given is a directory then this sensor will only return true
    :param google_cloud_conn_id: reference to the Google Connection
    :type google_cloud_conn_id: str
    :param bucket: Bucket name (relative to the base path set within the connection), can
        be a glob.
    :type bucket: str
    :param object: the object name which will be looked for in the google cloud storage
    :type object: str
    """

    def execute(self, context):
        print("printing poll inyetval", self.polling_interval, type(self.polling_interval))
        self.defer(
            timeout=self.execution_timeout,
            trigger=GCSTrigger(
                bucket=self.bucket,
                object_name=self.object,
                polling_period_seconds=self.polling_interval,
                google_cloud_conn_id=self.google_cloud_conn_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("File %s was found in bucket %s.", self.object, self.bucket)
        return event["message"]
