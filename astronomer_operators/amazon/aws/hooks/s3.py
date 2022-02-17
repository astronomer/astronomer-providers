# Copyright 2022 Astronomer Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from astronomer_operators.amazon.aws.hooks.base_aws_async import AwsBaseHookAsync

log = logging.getLogger(__name__)


class S3HookAsync(AwsBaseHookAsync):
    """
    Interact with AWS S3, using the aiobotocore library.
    """

    conn_type = "s3"
    hook_name = "S3"

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "s3"
        kwargs["resource_type"] = "s3"
        super().__init__(*args, **kwargs)
