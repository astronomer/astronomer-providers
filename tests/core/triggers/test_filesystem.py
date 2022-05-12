import asyncio

import pytest

from astronomer.providers.core.triggers.filesystem import FileTrigger


class TestFileTrigger:
    FILE_PATH = "/files/dags/example_async_file.py"
    POLL_INTERVAL = 3.0

    def test_serialization(self):
        """Asserts that the TaskStateTrigger correctly serializes its arguments and classpath."""
        trigger = FileTrigger(filepath=self.FILE_PATH, poll_interval=self.POLL_INTERVAL)
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.core.triggers.filesystem.FileTrigger"
        assert kwargs == {
            "filepath": self.FILE_PATH,
            "poll_interval": self.POLL_INTERVAL,
            "recursive": False,
        }

    @pytest.mark.asyncio
    async def test_task_file_trigger(self, tmp_path):
        """Asserts that the FileTrigger only goes off on or after file is found"""
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
