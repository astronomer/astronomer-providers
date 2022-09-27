import asyncio
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.triggers.base import TriggerEvent

from astronomer.providers.sftp.triggers.sftp import SFTPTrigger


class TestSFTPTrigger:
    def test_sftp_trigger_serialization(self):
        """
        Asserts that the SFTPTrigger correctly serializes its arguments and classpath.
        """
        trigger = SFTPTrigger(path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")
        classpath, kwargs = trigger.serialize()
        assert classpath == "astronomer.providers.sftp.triggers.sftp.SFTPTrigger"
        assert kwargs == {
            "path": "test/path/",
            "file_pattern": "my_test_file",
            "sftp_conn_id": "sftp_default",
            "poke_interval": 5.0,
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_file_by_pattern")
    async def test_sftp_trigger_run_trigger_success_state(self, mock_get_file_by_pattern):
        """
        Assert that a TriggerEvent with a success status is yielded if a file
        matching the pattern is returned by the hook
        """
        mock_get_file_by_pattern.return_value = "some_file"

        trigger = SFTPTrigger(path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")

        expected_event = {"status": "success", "message": "Sensed file: some_file"}

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert TriggerEvent(expected_event) == actual_event

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_file_by_pattern")
    async def test_sftp_trigger_run_trigger_defer_state(self, mock_get_file_by_pattern):
        """
        Assert that a the task does not complete if the hook raises an AirflowException,
        indicating that the task needs to be deferred
        """
        mock_get_file_by_pattern.side_effect = AirflowException("No files at path found...")

        trigger = SFTPTrigger(path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_file_by_pattern")
    async def test_sftp_trigger_run_trigger_failure_state(self, mock_get_file_by_pattern):
        """
        Assert that a TriggerEvent with a failure status is yielded if an exception
        other than an AirflowException is raised by the hook
        """
        mock_get_file_by_pattern.side_effect = Exception("An unexpected exception")

        trigger = SFTPTrigger(path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")

        expected_event = {"status": "failure", "message": "An unexpected exception"}

        with pytest.raises(Exception):
            generator = trigger.run()
            actual_event = await generator.asend(None)

            assert TriggerEvent(expected_event) == actual_event
