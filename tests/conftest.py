"""Shared pytest fixtures for snowcraft tests.

Unit test fixtures mock the Snowflake cursor entirely — no live connection is
needed or used. Integration test fixtures require the ``SNOWFLAKE_*`` environment
variables to be set and are automatically skipped when they are absent.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from snowcraft.connection import SnowcraftConnection

# ---------------------------------------------------------------------------
# Unit test fixtures — fully mocked, no real Snowflake connection
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_cursor(mocker: pytest.fixture) -> MagicMock:  # type: ignore[type-arg]
    """A MagicMock that behaves like a SnowflakeCursor."""
    cursor = mocker.MagicMock()
    cursor.sfqid = "mock-query-id-0001"
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    cursor.description = []
    return cursor


@pytest.fixture
def mock_conn(mocker: pytest.fixture, mock_cursor: MagicMock) -> MagicMock:  # type: ignore[type-arg]
    """A MagicMock that behaves like a SnowcraftConnection.

    The ``.execute()`` method returns ``mock_cursor`` by default. Individual
    tests can override ``mock_cursor.fetchone.return_value`` or
    ``mock_cursor.fetchall.return_value`` to simulate query results.
    """
    conn = mocker.MagicMock(spec=SnowcraftConnection)
    conn.execute.return_value = mock_cursor
    conn.cursor.return_value = mock_cursor
    return conn


# ---------------------------------------------------------------------------
# Integration test fixtures — require real Snowflake credentials
# ---------------------------------------------------------------------------

_INTEGRATION_ENV_VAR = "SNOWFLAKE_ACCOUNT"


def _integration_skip_reason() -> str:
    return (
        f"Integration tests require the {_INTEGRATION_ENV_VAR} environment variable "
        "to be set. Configure your test account credentials before running."
    )


@pytest.fixture(scope="session")
def integration_conn() -> SnowcraftConnection:  # type: ignore[return]
    """A live SnowcraftConnection for integration tests.

    Automatically skips the test when ``SNOWFLAKE_ACCOUNT`` is not set.
    The connection is opened once per test session and closed at teardown.
    """
    if not os.environ.get(_INTEGRATION_ENV_VAR):
        pytest.skip(_integration_skip_reason())

    conn = SnowcraftConnection()
    conn.connect()
    yield conn  # type: ignore[misc]
    conn.close()


@pytest.fixture(scope="session", autouse=False)
def integration_database(integration_conn: SnowcraftConnection) -> None:
    """Create the SNOWCRAFT_TEST database and tear it down after the session.

    This fixture is NOT autouse so integration test modules must request it
    explicitly with ``@pytest.mark.usefixtures("integration_database")``.
    Never run this fixture against a production account.
    """
    integration_conn.execute("CREATE DATABASE IF NOT EXISTS SNOWCRAFT_TEST")
    integration_conn.execute("CREATE SCHEMA IF NOT EXISTS SNOWCRAFT_TEST.PUBLIC")
    yield  # type: ignore[misc]
    integration_conn.execute("DROP DATABASE IF EXISTS SNOWCRAFT_TEST")
