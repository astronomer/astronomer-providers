import datetime
from unittest.mock import AsyncMock, patch

import pytest
from airflow.exceptions import AirflowException
from asyncssh import SFTPAttrs, SFTPNoSuchFile

from astronomer.providers.sftp.hooks.sftp import SFTPHookAsync


class MockSFTPClient:
    def __init__(self):
        pass

    async def listdir(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("File does not exist")
        else:
            return ["..", ".", "file"]

    async def stat(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("No files matching")
        else:
            sftp_obj = SFTPAttrs()
            sftp_obj.mtime = 1667302566
            return sftp_obj


class MockSSHClient:
    def __init__(self):
        pass

    async def start_sftp_client(self):
        return MockSFTPClient()


class MockAirflowConnection:
    def __init__(self, known_hosts="~/.ssh/known_hosts"):
        self.host = "localhost"
        self.port = 22
        self.login = "username"
        self.password = "password"
        self.extra = """
        {
            "key_file": "~/keys/my_key",
            "known_hosts": "unused",
            "passphrase": "mypassphrase"
        }
        """
        self.extra_dejson = {
            "key_file": "~/keys/my_key",
            "known_hosts": known_hosts,
            "passphrase": "mypassphrase",
        }

    def extra_dejson(self):
        return self.extra


class TestSFTPHookAsync:
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_building_known_hosts_none(
        self, mock_get_connection, mock_connect
    ):
        """
        Assert that connection details passed through the extra field in the Airflow connection
        are properly passed when creating SFTP connection
        """

        mock_get_connection.return_value = MockAirflowConnection(known_hosts="None")

        hook = SFTPHookAsync()
        await hook._get_conn()

        expected_connection_details = {
            "host": "localhost",
            "port": 22,
            "username": "username",
            "password": "password",
            "client_keys": "~/keys/my_key",
            "known_hosts": None,
            "passphrase": "mypassphrase",
        }

        mock_connect.assert_called_with(**expected_connection_details)

    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_building(self, mock_get_connection, mock_connect):
        """
        Assert that connection details passed through the extra field in the Airflow connection
        are properly passed when creating SFTP connection
        """

        mock_get_connection.return_value = MockAirflowConnection()

        hook = SFTPHookAsync()
        await hook._get_conn()

        expected_connection_details = {
            "host": "localhost",
            "port": 22,
            "username": "username",
            "password": "password",
            "client_keys": "~/keys/my_key",
            "known_hosts": "~/.ssh/known_hosts",
            "passphrase": "mypassphrase",
        }

        mock_connect.assert_called_with(**expected_connection_details)

    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    @pytest.mark.asyncio
    async def test_list_directory_path_does_not_exist(self, mock_hook_get_conn):
        """
        Assert that AirflowException is raised when path does not exist on SFTP server
        """
        mock_hook_get_conn.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        expected_files = None
        files = await hook.list_directory(path="/path/does_not/exist/")
        assert files == expected_files

    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    @pytest.mark.asyncio
    async def test_list_directory_path_has_files(self, mock_hook_get_conn):
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
    async def test_get_file_by_pattern_with_match(self, mock_hook_get_conn):
        """
        Assert that filename is returned when file pattern is matched on SFTP server
        """
        mock_hook_get_conn.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        file = await hook.get_file_by_pattern(path="/path/exists/", fnmatch_pattern="file")

        assert file == "file"

    @pytest.mark.asyncio
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    async def test_get_file_by_pattern_with_no_match(self, mock_hook_get_conn):
        """
        Assert that AirflowException is raised when no files match file pattern on SFTP server
        """
        mock_hook_get_conn.return_value = MockSSHClient()
        hook = SFTPHookAsync()

        with pytest.raises(AirflowException) as exc:
            await hook.get_file_by_pattern(path="/path/exists/", fnmatch_pattern="file_does_not_exist")

        assert str(exc.value) == "No files matching file pattern were found at /path/exists/ — Deferring"

    @pytest.mark.asyncio
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    async def test_get_mod_time(self, mock_hook_get_conn):
        """
        Assert that file attribute and return the modified time of the file
        """
        mock_hook_get_conn.return_value.start_sftp_client.return_value = MockSFTPClient()
        hook = SFTPHookAsync()
        mod_time = await hook.get_mod_time("/path/exists/file")
        expected_value = datetime.datetime.fromtimestamp(1667302566).strftime("%Y%m%d%H%M%S")
        assert mod_time == expected_value

    @pytest.mark.asyncio
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    async def test_get_mod_time_exception(self, mock_hook_get_conn):
        """
        Assert that get_mod_time raise exception when file does not exist
        """
        mock_hook_get_conn.return_value.start_sftp_client.return_value = MockSFTPClient()
        hook = SFTPHookAsync()
        with pytest.raises(AirflowException) as exc:
            await hook.get_mod_time("/path/does_not/exist/")
        assert str(exc.value) == "No files matching"
