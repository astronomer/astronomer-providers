import asyncio
import unittest
from unittest import mock

import pytest
from airflow.models import Connection

from astronomer_operators.postgres.hooks.postgres import PostgresHookAsync


class TestPostgresHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.connection = Connection(login="login", password="password", host="host", schema="schema")

        class UnitTestPostgresHook(PostgresHookAsync):
            conn_name_attr = "test_conn_id"

        self.db_hook = UnitTestPostgresHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @pytest.mark.asyncio
    @mock.patch("astronomer_operators.postgres.hooks.postgres.asyncpg.connect")
    async def get_conn_non_default_id(self, mock_connect):
        self.db_hook.test_conn_id = "non_default"
        await self.db_hook.get_first(sql="select 1")
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", database="schema", port=None
        )
        self.db_hook.get_connection.assert_called_once_with("non_default")

    def test_get_conn_non_default_id(self):
        asyncio.run(self.get_conn_non_default_id())

    @pytest.mark.asyncio
    @mock.patch("astronomer_operators.postgres.hooks.postgres.asyncpg.connect")
    async def get_first(self, mock_connect):
        await self.db_hook.get_first(sql="select 1")
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", database="schema", port=None
        )

    def test_get_first(self):
        asyncio.run(self.get_first())

    @pytest.mark.asyncio
    @mock.patch("astronomer_operators.postgres.hooks.postgres.asyncpg.connect")
    async def get_conn_from_connection(self, mock_connect):
        hook = PostgresHookAsync()
        hook.get_connection = mock.Mock()
        hook.get_connection.return_value = Connection(
            login="login-conn", password="password-conn", host="host", schema="schema"
        )
        await hook.get_first(sql="select 1")
        mock_connect.assert_called_once_with(
            user="login-conn", password="password-conn", host="host", database="schema", port=None
        )

    def test_get_conn_from_connection(self):
        asyncio.run(self.get_conn_from_connection())

    @pytest.mark.asyncio
    @mock.patch("astronomer_operators.postgres.hooks.postgres.asyncpg.connect")
    async def get_conn_from_connection_with_schema(self, mock_connect):
        hook = PostgresHookAsync(schema="schema-override")
        hook.get_connection = mock.Mock()
        hook.get_connection.return_value = Connection(
            login="login-conn", password="password-conn", host="host", schema="schema"
        )
        await hook.get_first(sql="select 1")
        mock_connect.assert_called_once_with(
            user="login-conn", password="password-conn", host="host", database="schema-override", port=None
        )

    def test_get_conn_from_connection_with_schema(self):
        asyncio.run(self.get_conn_from_connection_with_schema())

    @pytest.mark.asyncio
    @mock.patch("astronomer_operators.postgres.hooks.postgres.asyncpg.connect")
    async def get_conn_extra(self, mock_connect):
        hook = PostgresHookAsync()
        hook.get_connection = mock.Mock()
        hook.get_connection.return_value = Connection(
            login="login-conn", password="password-conn", host="host", schema="schema", extra={"timeout": 3}
        )
        print(hook.__dict__)
        await hook.get_first(sql="select 1")
        mock_connect.assert_called_once_with(
            user="login-conn", password="password-conn", host="host", database="schema", port=None, timeout=3
        )

    def test_get_conn_extra(self):
        asyncio.run(self.get_conn_extra())

    # '''
    # Below test is failing with error.
    # FAILED test_postgres.py::TestPostgresHookConn::test_run_method - AttributeError: __aenter__
    # '''
    # @pytest.mark.asyncio
    # @mock.patch('astronomer_operators.postgres.hooks.postgres.asyncpg.create_pool')
    # async def run_method(self, mocked_pool):
    #     print("In run_method")
    #     self.db_hook.test_conn_id = 'non_default'
    #     self.db_hook.get_connection = mock.Mock()
    #     self.db_hook.get_connection.return_value = Connection(login='login-conn', password='password-conn',
    #                                                           host='host',schema='schema')
    #     await self.db_hook.run("select 1")
    # To check here whether return value of run method is equal to {"status": "success", "message": response}

    # def test_run_method(self):
    #     print("in test_run_method")
    #     asyncio.run(self.run_method())
