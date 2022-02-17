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

from astronomer_operators.http.triggers.http import HttpTrigger


def test_http_trigger_serialization():
    """
    Asserts that the HttpTrigger correctly serializes its arguments and classpath.
    """
    trigger = HttpTrigger(
        endpoint="test-endpoint",
        http_conn_id="http_default",
        method="GET",
        headers={"Content-Type": "application/json"},
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.http.triggers.http.HttpTrigger"
    assert kwargs == {
        "data": None,
        "endpoint": "test-endpoint",
        "extra_options": {},
        "headers": {"Content-Type": "application/json"},
        "http_conn_id": "http_default",
        "poll_interval": 5.0,
    }
