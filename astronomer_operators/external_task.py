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

import warnings

from astronomer_operators.core.sensors.external_task import ExternalTaskSensorAsync
from astronomer_operators.core.triggers.external_task import (
    DagStateTrigger,
    TaskStateTrigger,
)

DEPRECATED_NAMES = {
    "ExternalTaskSensorAsync": "astronomer_operators.core.sensors.external_task",
    "DagStateTrigger": "astronomer_operators.core.triggers.external_task",
    "TaskStateTrigger": "astronomer_operators.core.triggers.external_task",
}


def __getattr__(name):
    if name in DEPRECATED_NAMES:
        mod = DEPRECATED_NAMES[name]
        warnings.warn(
            f"Importing {name} from `astronomer_operators.external_task` is deprecated; please use `{mod}`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]


__all__ = ["DagStateTrigger", "ExternalTaskSensorAsync", "TaskStateTrigger"]
