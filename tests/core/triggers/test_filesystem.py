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

import asyncio
import sys

import pytest

from astronomer_operators.core.triggers.filesystem import FileTrigger

TEST_POLL_INTERVAL = 3.0


def test_filesystem_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = FileTrigger(filepath="/files/dags/example_async_file.py", poll_interval=TEST_POLL_INTERVAL)
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.core.triggers.filesystem.FileTrigger"
    assert kwargs == {
        "filepath": "/files/dags/example_async_file.py",
        "poll_interval": TEST_POLL_INTERVAL,
        "recursive": False,
    }


@pytest.mark.skipif(
    sys.version_info.minor <= 6 and sys.version_info.major <= 3,
    reason="No async on 3.6",
)
@pytest.mark.asyncio
async def test_task_file_trigger(tmp_path):
    """
    Asserts that the FileTrigger only goes off on or after file is found
    """
    tmp_dir = tmp_path / "test_dir"
    tmp_dir.mkdir()
    p = tmp_dir / "hello.txt"
    p.unlink(missing_ok=True)

    trigger = FileTrigger(
        filepath=str(p.resolve()),
        poll_interval=0.2,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # It should not have produced a result
    assert task.done() is False

    p.touch()

    await asyncio.sleep(0.5)
    assert task.done() is True

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()
