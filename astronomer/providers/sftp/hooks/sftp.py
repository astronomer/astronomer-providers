from __future__ import annotations

from base64 import decodebytes
from datetime import datetime
from fnmatch import fnmatch
from typing import Any

import asyncssh
import paramiko
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async
from paramiko.config import SSH_PORT
from tenacity import Retrying, stop_after_attempt, wait_fixed, wait_random


class SFTPHookAsync(BaseHook):
    """
    Interact with an SFTP server via asyncssh package

    :param sftp_conn_id: SFTP connection ID to be used for connecting to SFTP server
    :param host: hostname of the SFTP server
    :param port: port of the SFTP server
    :param username: username used when authenticating to the SFTP server
    :param password: password used when authenticating to the SFTP server
                     Can be left blank if using a key file
    :param known_hosts: path to the known_hosts file on the local file system
                     If known_hosts is set to the literal "none", then no host verification is performed
    :param key_file: path to the client key file used for authentication to SFTP server
    :param passphrase: passphrase used with the key_file for authentication to SFTP server
    """

    conn_name_attr = "ssh_conn_id"
    default_conn_name = "sftp_default"
    conn_type = "sftp"
    hook_name = "SFTP"
    default_known_hosts = "none"

    _host_key_mappings = {
        "rsa": paramiko.RSAKey,
        "dss": paramiko.DSSKey,
        "ecdsa": paramiko.ECDSAKey,
        "ed25519": paramiko.Ed25519Key,
    }

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
        self.sftp_conn_id = sftp_conn_id
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.known_hosts = known_hosts
        self.key_file = key_file
        self.passphrase = passphrase
        self.private_key = private_key
        self.no_host_key_check = True
        self.host_key = None

    async def _get_conn(self) -> asyncssh.SSHClientConnection:
        """
        Asynchronously connect to the SFTP server as an SSH client

        The following parameters are provided either in the extra json object in
        the SFTP connection definition

        - key_file
        - known_hosts
        - passphrase
        """
        if self.sftp_conn_id is not None:
            conn = await sync_to_async(self.get_connection)(self.sftp_conn_id)

            if conn.extra is not None:
                extra_options = conn.extra_dejson

                if "key_file" in extra_options and self.key_file == "":
                    self.key_file = extra_options.get("key_file")

                if "known_hosts" in extra_options:
                    self.known_hosts = extra_options.get("known_hosts")

                if ("passphrase" or "private_key_passphrase") in extra_options:
                    self.passphrase = extra_options.get("passphrase")

                if "private_key" in extra_options:
                    self.private_key = extra_options.get("private_key")

                host_key = extra_options.get("host_key")
                no_host_key_check = extra_options.get("no_host_key_check")

                if no_host_key_check is not None:
                    no_host_key_check = str(no_host_key_check).lower() == "true"
                    if host_key is not None and no_host_key_check:
                        raise ValueError("Must check host key when provided")

                    self.no_host_key_check = no_host_key_check

                if host_key is not None:
                    if host_key.startswith("ssh-"):
                        key_type, host_key = host_key.split(None)[:2]
                        key_constructor = self._host_key_mappings[key_type[4:]]
                    else:
                        key_constructor = paramiko.RSAKey
                    decoded_host_key = decodebytes(host_key.encode("utf-8"))
                    self.host_key = key_constructor(data=decoded_host_key)
                    self.no_host_key_check = False

            if self.no_host_key_check:
                self.log.warning(
                    "No Host Key Verification. This won't protect against Man-In-The-Middle attacks"
                )
            elif self.host_key is not None:
                # The asyncssh client we use does not support host key verification other than the ones given in
                # known_hosts file(Ref: https://github.com/ronf/asyncssh/issues/176). Hence, we use a paramiko client to
                # validate the provided host key before proceeding for further operation.
                with paramiko.SSHClient() as paramiko_client:
                    client_host_keys = paramiko_client.get_host_keys()
                    if conn.port == SSH_PORT:
                        client_host_keys.add(conn.host, self.host_key.get_name(), self.host_key)
                    else:
                        client_host_keys.add(
                            f"[{conn.host}]:{conn.port}", self.host_key.get_name(), self.host_key
                        )
                    connect_kwargs: dict[str, Any] = {
                        "hostname": conn.host,
                        "username": conn.login,
                        "password": conn.password,
                        "port": conn.port,
                    }

                    if self.private_key:
                        connect_kwargs.update(pkey=self.private_key)

                    if self.key_file:
                        connect_kwargs.update(key_filename=self.key_file)

                    def log_before_sleep(retry_state):
                        return self.log.info(
                            "Failed to connect. Sleeping before retry attempt %d", retry_state.attempt_number
                        )

                    for attempt in Retrying(
                        reraise=True,
                        wait=wait_fixed(3) + wait_random(0, 2),
                        stop=stop_after_attempt(3),
                        before_sleep=log_before_sleep,
                    ):
                        with attempt:
                            paramiko_client.connect(**connect_kwargs)

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

            ssh_client = await asyncssh.connect(**conn_config)

            return ssh_client

    async def list_directory(self, path: str = "") -> list[str] | None:
        """Returns a list of files on the SFTP server at the provided path"""
        ssh_conn = await self._get_conn()
        sftp_client = await ssh_conn.start_sftp_client()
        try:
            files = await sftp_client.listdir(path)
            return sorted(files)
        except asyncssh.SFTPNoSuchFile:
            return None

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
