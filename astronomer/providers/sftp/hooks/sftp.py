from __future__ import annotations

import os.path
import warnings
from datetime import datetime
from fnmatch import fnmatch
from typing import Sequence

import asyncssh
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from asgiref.sync import sync_to_async


class SFTPHookAsync(BaseHook):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.sftp.hooks.sftp.SFTPHookAsync` instead.
    """

    conn_name_attr = "ssh_conn_id"
    default_conn_name = "sftp_default"
    conn_type = "sftp"
    hook_name = "SFTP"
    default_known_hosts = "~/.ssh/known_hosts"

    def __init__(  # nosec: B107
        self,
        sftp_conn_id: str = default_conn_name,
        host: str = "",
        port: int = 22,
        username: str = "",
        password: str = "",
        known_hosts: str = default_known_hosts,
        key_file: str = "",
        passphrase: str = "",
        private_key: str = "",
    ) -> None:
        warnings.warn(
            "This class is deprecated and will be removed in 2.0.0. "
            "Use `airflow.providers.sftp.hooks.sftp.SFTPHookAsync` instead."
        )
        self.sftp_conn_id = sftp_conn_id
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.known_hosts: bytes | str = os.path.expanduser(known_hosts)
        self.key_file = key_file
        self.passphrase = passphrase
        self.private_key = private_key

    def _parse_extras(self, conn: Connection) -> None:
        """Parse extra fields from the connection into instance fields"""
        extra_options = conn.extra_dejson
        if "key_file" in extra_options and self.key_file == "":
            self.key_file = extra_options["key_file"]
        if "known_hosts" in extra_options and self.known_hosts != self.default_known_hosts:
            self.known_hosts = extra_options["known_hosts"]
        if ("passphrase" or "private_key_passphrase") in extra_options:
            self.passphrase = extra_options["passphrase"]
        if "private_key" in extra_options:
            self.private_key = extra_options["private_key"]

        host_key = extra_options.get("host_key")
        no_host_key_check = extra_options.get("no_host_key_check")

        if no_host_key_check is not None:
            no_host_key_check = str(no_host_key_check).lower() == "true"
            if host_key is not None and no_host_key_check:
                raise ValueError("Host key check was skipped, but `host_key` value was given")
            if no_host_key_check:
                self.log.warning(
                    "No Host Key Verification. This won't protect against Man-In-The-Middle attacks"
                )
                self.known_hosts = "none"

        if host_key is not None:
            self.known_hosts = f"{conn.host} {host_key}".encode()

    async def _get_conn(self) -> asyncssh.SSHClientConnection:
        """
        Asynchronously connect to the SFTP server as an SSH client

        The following parameters are provided either in the extra json object in
        the SFTP connection definition

        - key_file
        - known_hosts
        - passphrase
        """
        conn = await sync_to_async(self.get_connection)(self.sftp_conn_id)
        if conn.extra is not None:
            self._parse_extras(conn)

        conn_config = {
            "host": conn.host,
            "port": conn.port,
            "username": conn.login,
            "password": conn.password,
        }
        if self.key_file:
            conn_config.update(client_keys=self.key_file)
        if self.known_hosts:
            if self.known_hosts.lower() == "none":
                conn_config.update(known_hosts=None)
            else:
                conn_config.update(known_hosts=self.known_hosts)
        if self.private_key:
            _private_key = asyncssh.import_private_key(self.private_key, self.passphrase)
            conn_config.update(client_keys=[_private_key])
        if self.passphrase:
            conn_config.update(passphrase=self.passphrase)
        ssh_client_conn = await asyncssh.connect(**conn_config)
        return ssh_client_conn

    async def list_directory(self, path: str = "") -> list[str] | None:
        """Returns a list of files on the SFTP server at the provided path"""
        ssh_conn = await self._get_conn()
        sftp_client = await ssh_conn.start_sftp_client()
        try:
            files = await sftp_client.listdir(path)
            return sorted(files)
        except asyncssh.SFTPNoSuchFile:
            return None

    async def read_directory(self, path: str = "") -> Sequence[asyncssh.sftp.SFTPName] | None:
        """Returns a list of files along with their attributes on the SFTP server at the provided path"""
        ssh_conn = await self._get_conn()
        sftp_client = await ssh_conn.start_sftp_client()
        try:
            files = await sftp_client.readdir(path)
            return files
        except asyncssh.SFTPNoSuchFile:
            return None

    async def get_files_and_attrs_by_pattern(
        self, path: str = "", fnmatch_pattern: str = ""
    ) -> Sequence[asyncssh.sftp.SFTPName]:
        """
        Returns the files along with their attributes matching the file pattern (e.g. ``*.pdf``) at the provided path,
        if one exists. Otherwise, raises an AirflowException to be handled upstream for deferring
        """
        files_list = await self.read_directory(path)
        if files_list is None:
            raise FileNotFoundError(f"No files at path {path!r} found...")
        matched_files = [file for file in files_list if fnmatch(str(file.filename), fnmatch_pattern)]
        return matched_files

    async def get_files_by_pattern(self, path: str = "", fnmatch_pattern: str = "") -> list[str]:
        """
        Returns the name of a file matching the file pattern at the provided path, if one exists
        Otherwise, raises an AirflowException to be handled upstream for deferring
        """
        files_list = await self.list_directory(path)
        if files_list is None:
            raise AirflowException(f"No files at path {path} found...")
        matched_files = [file for file in files_list if fnmatch(file, fnmatch_pattern)]
        return matched_files

    async def get_mod_time(self, path: str) -> str:
        """
        Makes SFTP async connection and looks for last modified time in the specific file
        path and returns last modification time for the file path.

        :param path: full path to the remote file
        """
        ssh_conn = await self._get_conn()
        sftp_client = await ssh_conn.start_sftp_client()
        try:
            ftp_mdtm = await sftp_client.stat(path)
            modified_time = ftp_mdtm.mtime
            mod_time = datetime.fromtimestamp(modified_time).strftime("%Y%m%d%H%M%S")  # type: ignore[arg-type]
            self.log.info("Found File %s last modified: %s", str(path), str(mod_time))
            return mod_time
        except asyncssh.SFTPNoSuchFile:
            raise AirflowException("No files matching")
