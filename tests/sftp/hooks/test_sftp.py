from __future__ import annotations

import datetime
import os.path
from unittest.mock import AsyncMock, patch

import paramiko.ssh_exception
import pytest
from airflow.exceptions import AirflowException
from asyncssh import SFTPAttrs, SFTPNoSuchFile
from asyncssh.sftp import SFTPName

from astronomer.providers.sftp.hooks.sftp import SFTPHookAsync


class MockSFTPClient:
    def __init__(self):
        pass

    async def listdir(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("File does not exist")
        else:
            return ["..", ".", "file"]

    async def readdir(self, path: str):
        if path == "/path/does_not/exist/":
            raise SFTPNoSuchFile("File does not exist")
        else:
            return [SFTPName(".."), SFTPName("."), SFTPName("file")]

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


class MockAirflowConnectionWithHostKey:
    def __init__(self, host_key: str | None = None, no_host_key_check: bool = False, port: int = 22):
        self.host = "localhost"
        self.port = port
        self.login = "username"
        self.password = "password"
        self.extra = f'{{ "no_host_key_check": {no_host_key_check}, "host_key": {host_key} }}'
        self.extra_dejson = {
            "no_host_key_check": no_host_key_check,
            "host_key": host_key,
            "key_file": "~/keys/my_key",
            "private_key": "~/keys/my_key",
        }

    def extra_dejson(self):
        return self.extra


class MockAirflowConnectionWithPrivate:
    def __init__(self):
        self.host = "localhost"
        self.port = 22
        self.login = "username"
        self.password = "password"
        self.extra = """
                {
                    "private_key": "~/keys/my_key",
                    "known_hosts": "unused",
                    "passphrase": "mypassphrase"
                }
                """
        self.extra_dejson = {
            "private_key": "~/keys/my_key",
            "known_hosts": None,
            "passphrase": "mypassphrase",
        }

    def extra_dejson(self):
        return self.extra


class TestSFTPHookAsync:
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._validate_host_key_using_paramiko")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_building_known_hosts_none(
        self, mock_paramiko_validation, mock_get_connection, mock_connect, caplog
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

    @pytest.mark.parametrize(
        "mock_port, mock_host_key",
        [
            (22, "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"),
            (2222, "AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"),
            (
                2222,
                "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBDDsXFe87LsBA1Hfi+mtw"
                "/EoQkv8bXVtfOwdMP1ETpHVsYpm5QG/7tsLlKdE8h6EoV/OFw7XQtoibNZp/l5ABjE=",
            ),
        ],
    )
    @patch("paramiko.SSHClient.connect")
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("asyncssh.import_private_key")
    @patch("paramiko.RSAKey")
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_with_host_key(
        self,
        mock_get_connection,
        mock_paramiko_constructor,
        mock_import_private_key,
        mock_connect,
        mock_paramiko_connect,
        mock_port,
        mock_host_key,
    ):
        """
        Assert that connection details passed through the extra field in the Airflow connection
        are properly passed to paramiko client for validating given host key.
        """
        mock_get_connection.return_value = MockAirflowConnectionWithHostKey(
            host_key=mock_host_key, no_host_key_check=False, port=mock_port
        )

        hook = SFTPHookAsync()
        await hook._get_conn()

        expected_connection_details = {
            "hostname": "localhost",
            "port": mock_port,
            "username": "username",
            "password": "password",
            "pkey": "~/keys/my_key",
            "key_filename": "~/keys/my_key",
        }

        if len(mock_host_key.split(" ")) > 1:
            assert hook.host_key.get_name() == mock_host_key.split(" ")[0]
            assert hook.host_key.get_base64() == mock_host_key.split(" ")[1]
        mock_paramiko_connect.assert_called_with(**expected_connection_details)

    @patch("paramiko.SSHClient.connect")
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_raises_valuerror(
        self, mock_get_connection, mock_connect, mock_paramiko_connect
    ):
        """
        Assert that when both host_key and no_host_key_check are set, a valuerror is raised because no_host_key_check
        should be unset when host_key is given and the host_key needs to be validated.
        """
        host_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"
        mock_get_connection.return_value = MockAirflowConnectionWithHostKey(
            host_key=host_key, no_host_key_check=True
        )

        hook = SFTPHookAsync()
        with pytest.raises(ValueError) as exc:
            await hook._get_conn()

        assert str(exc.value) == "Must check host key when provided."

    @patch("paramiko.SSHClient.connect")
    @patch("asyncssh.import_private_key")
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_no_host_key_check_set_logs_warning(
        self, mock_get_connection, mock_connect, mock_import_pkey, mock_ssh_connect, caplog
    ):
        """Assert that when no_host_key_check is set, a warning is logged for MITM attacks possibility."""
        mock_get_connection.return_value = MockAirflowConnectionWithHostKey(no_host_key_check=True)

        hook = SFTPHookAsync()
        await hook._get_conn()
        assert "No Host Key Verification. This won't protect against Man-In-The-Middle attacks" in caplog.text

    @patch("paramiko.SSHClient.connect")
    @patch("asyncssh.import_private_key")
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_host_key_validation_exception_message(
        self, mock_get_connection, mock_connect, mock_import_pkey, mock_paramiko_connect
    ):
        """Assert expected failure logs when host key validation fails."""
        host_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"
        mock_airflow_connection = MockAirflowConnectionWithHostKey(host_key=host_key)
        mock_get_connection.return_value = mock_airflow_connection

        mock_paramiko_connect.side_effect = paramiko.ssh_exception.SSHException()
        hook = SFTPHookAsync()
        with pytest.raises(paramiko.ssh_exception.SSHException) as exc:
            await hook._get_conn()
        assert f"Given host key '{host_key}' for server 'localhost' does not match!" in str(exc.value)
        known_hosts_path = "~/.ssh/known_hosts"
        assert (
            f"None of the host keys in the 'known_hosts' file "
            f"'{os.path.expanduser(known_hosts_path)}' match!"
        ) in str(exc.value)
        print(exc.value)

    @patch("paramiko.SSHClient.connect", side_effect=Exception("mocked error"))
    @patch("asyncssh.import_private_key")
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_retry_upon_error(
        self, mock_get_connection, mock_connect, mock_import_pkey, mock_paramiko_connect
    ):
        """
        Assert than paramiko connect is retried upon failure.
        """
        host_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFe8P8lk5HFfL/rMlcCMHQhw1cg+uZtlK5rXQk2C4pOY"
        mock_get_connection.return_value = MockAirflowConnectionWithHostKey(
            host_key=host_key, no_host_key_check=False
        )
        hook = SFTPHookAsync()
        with pytest.raises(Exception):
            await hook._get_conn()

        assert len(mock_paramiko_connect.mock_calls) == 3

    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._validate_host_key_using_paramiko")
    @pytest.mark.asyncio
    async def test_extra_dejson_fields_for_connection_building(
        self, mock_paramiko_validation, mock_get_connection, mock_connect
    ):
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

    @pytest.mark.asyncio
    @patch("asyncssh.connect", new_callable=AsyncMock)
    @patch("asyncssh.import_private_key")
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync.get_connection")
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._validate_host_key_using_paramiko")
    async def test_connection_private(
        self, mock_paramiko_validation, mock_get_connection, mock_import_private_key, mock_connect
    ):
        """
        Assert that connection details with private key passed through the extra field in the Airflow connection
        are properly passed when creating SFTP connection
        """

        mock_get_connection.return_value = MockAirflowConnectionWithPrivate()
        mock_import_private_key.return_value = "test"

        hook = SFTPHookAsync()
        await hook._get_conn()

        expected_connection_details = {
            "host": "localhost",
            "port": 22,
            "username": "username",
            "password": "password",
            "client_keys": ["test"],
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

        files = await hook.get_files_by_pattern(path="/path/exists/", fnmatch_pattern="file")

        assert len(files) == 1
        assert files[0].filename == "file"

    @pytest.mark.asyncio
    @patch("astronomer.providers.sftp.hooks.sftp.SFTPHookAsync._get_conn")
    async def test_get_file_by_pattern_with_no_match(self, mock_hook_get_conn):
        """
        Assert that AirflowException is raised when no files match file pattern on SFTP server
        """
        mock_hook_get_conn.return_value = MockSSHClient()
        hook = SFTPHookAsync()
        file = await hook.get_files_by_pattern(path="/path/exists/", fnmatch_pattern="file_does_not_exist")

        assert file == []

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
