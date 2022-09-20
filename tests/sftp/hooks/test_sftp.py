from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException
from asyncssh import SFTPNoSuchFile

from astronomer.providers.sftp.hooks.sftp import SFTPHookAsync


class MockSFTPClient:
    def __init__(self):
        pass

    async def listdir(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("File does not exist")
        else:
            return ["..", ".", "file"]


class MockSSHClient:
    def __init__(self):
        pass

    async def start_sftp_client(self):
        return MockSFTPClient()


@patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
@pytest.mark.asyncio
async def test_list_directory_path_does_not_exist(mock_hook_get_conn):
    """
    Assert that AirflowException is raised when path does not exist on SFTP server
    """
    mock_hook_get_conn.return_value = MockSSHClient()
    hook = SFTPHookAsync()

    with pytest.raises(AirflowException) as exc:
        await hook.list_directory(path="/path/does_not/exist/")

    assert str(exc.value) == "No files at path /path/does_not/exist/ found — Deferring"


@patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
@pytest.mark.asyncio
async def test_list_directory_path_has_files(mock_hook_get_conn):
    """
    Assert that file list is returned when path exists on SFTP server
    """
    mock_hook_get_conn.return_value = MockSSHClient()
    hook = SFTPHookAsync()

    expected_files = ["..", ".", "file"]
    files = await hook.list_directory(path="/path/exists/")
    assert sorted(files) == sorted(expected_files)


@patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
@pytest.mark.asyncio
async def test_get_file_by_pattern_with_match(mock_hook_get_conn):
    """
    Assert that filename is returned when file pattern is matched on SFTP server
    """
    mock_hook_get_conn.return_value = MockSSHClient()
    hook = SFTPHookAsync()

    file = await hook.get_file_by_pattern(path="/path/exists/", fnmatch_pattern="file")

    assert file == "file"


@patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
@pytest.mark.asyncio
async def test_get_file_by_pattern_with_no_match(mock_hook_get_conn):
    """
    Assert that AirflowException is raised when no files match file pattern on SFTP server
    """
    mock_hook_get_conn.return_value = MockSSHClient()
    hook = SFTPHookAsync()

    with pytest.raises(AirflowException) as exc:
        await hook.get_file_by_pattern(path="/path/exists/", fnmatch_pattern="file_does_not_exist")

    assert str(exc.value) == "No files matching file pattern were found at /path/exists/ — Deferring"
