"""Unit tests for snowcraft.connection.

Tests mock snowflake.connector.connect() entirely — no real Snowflake
connection is used.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from snowcraft.connection import SnowforgeConnection
from snowcraft.exceptions import ConnectionError as SnowforgeConnectionError

# ---------------------------------------------------------------------------
# Construction and validation
# ---------------------------------------------------------------------------


class TestSnowforgeConnectionConstruction:
    def test_explicit_credentials_accepted(self) -> None:
        conn = SnowforgeConnection(account="acct", user="usr", password="pw")
        assert conn._account == "acct"
        assert conn._user == "usr"

    def test_missing_account_raises(self) -> None:
        with pytest.raises(SnowforgeConnectionError, match="account"):
            SnowforgeConnection(user="u", password="p")

    def test_missing_user_raises(self) -> None:
        with pytest.raises(SnowforgeConnectionError, match="user"):
            SnowforgeConnection(account="a", password="p")

    def test_missing_password_raises(self) -> None:
        with pytest.raises(SnowforgeConnectionError, match="password"):
            SnowforgeConnection(account="a", user="u")

    def test_env_var_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
        monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
        conn = SnowforgeConnection()
        assert conn._account == "env_acct"
        assert conn._user == "env_user"

    def test_explicit_overrides_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_acct")
        monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env_pw")
        conn = SnowforgeConnection(account="explicit")
        assert conn._account == "explicit"

    def test_optional_params_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "a")
        monkeypatch.setenv("SNOWFLAKE_USER", "u")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "DB")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "WH")
        monkeypatch.setenv("SNOWFLAKE_ROLE", "ROLE")
        conn = SnowforgeConnection()
        assert conn._database == "DB"
        assert conn._warehouse == "WH"
        assert conn._role == "ROLE"

    def test_multiple_missing_params_listed(self) -> None:
        with pytest.raises(SnowforgeConnectionError) as exc_info:
            SnowforgeConnection()
        msg = str(exc_info.value)
        assert "account" in msg
        assert "user" in msg
        assert "password" in msg


# ---------------------------------------------------------------------------
# _build_connect_kwargs
# ---------------------------------------------------------------------------


class TestBuildConnectKwargs:
    def _conn(self, **kwargs: str) -> SnowforgeConnection:
        defaults = {"account": "a", "user": "u", "password": "p"}
        defaults.update(kwargs)
        return SnowforgeConnection(**defaults)

    def test_required_params_always_present(self) -> None:
        kw = self._conn()._build_connect_kwargs()
        assert "account" in kw
        assert "user" in kw
        assert "password" in kw

    def test_optional_params_included_when_set(self) -> None:
        kw = self._conn(database="DB", schema="S", warehouse="WH", role="R")._build_connect_kwargs()
        assert kw["database"] == "DB"
        assert kw["schema"] == "S"
        assert kw["warehouse"] == "WH"
        assert kw["role"] == "R"

    def test_optional_params_excluded_when_absent(self) -> None:
        kw = self._conn()._build_connect_kwargs()
        assert "database" not in kw
        assert "schema" not in kw
        assert "warehouse" not in kw
        assert "role" not in kw


# ---------------------------------------------------------------------------
# connect() / close()
# ---------------------------------------------------------------------------


class TestConnectAndClose:
    def _conn(self) -> SnowforgeConnection:
        return SnowforgeConnection(account="a", user="u", password="p")

    def test_connect_calls_snowflake_connector(self) -> None:
        conn = self._conn()
        mock_raw = MagicMock()
        with patch("snowflake.connector.connect", return_value=mock_raw) as mock_connect:
            conn.connect()
            mock_connect.assert_called_once()
            assert conn._raw_conn is mock_raw

    def test_connect_failure_raises_connection_error(self) -> None:
        import snowflake.connector

        conn = self._conn()
        with patch(
            "snowflake.connector.connect",
            side_effect=snowflake.connector.Error("bad creds"),
        ):
            with pytest.raises(SnowforgeConnectionError, match="Failed to connect"):
                conn.connect()

    def test_close_calls_underlying_close(self) -> None:
        conn = self._conn()
        mock_raw = MagicMock()
        conn._raw_conn = mock_raw
        conn.close()
        mock_raw.close.assert_called_once()
        assert conn._raw_conn is None

    def test_close_is_noop_when_not_connected(self) -> None:
        conn = self._conn()
        # Should not raise
        conn.close()


# ---------------------------------------------------------------------------
# cursor() and execute()
# ---------------------------------------------------------------------------


class TestCursorAndExecute:
    def _connected_conn(self) -> SnowforgeConnection:
        conn = SnowforgeConnection(account="a", user="u", password="p")
        conn._raw_conn = MagicMock()
        return conn

    def test_cursor_returns_snowflake_cursor(self) -> None:
        conn = self._connected_conn()
        mock_cursor = MagicMock()
        conn._raw_conn.cursor.return_value = mock_cursor
        assert conn.cursor() is mock_cursor

    def test_cursor_raises_when_not_connected(self) -> None:
        conn = SnowforgeConnection(account="a", user="u", password="p")
        with pytest.raises(SnowforgeConnectionError, match="not open"):
            conn.cursor()

    def test_execute_calls_cursor_execute(self) -> None:
        conn = self._connected_conn()
        mock_cursor = MagicMock()
        conn._raw_conn.cursor.return_value = mock_cursor
        conn.execute("SELECT 1")
        mock_cursor.execute.assert_called_once_with("SELECT 1", None)

    def test_execute_passes_params(self) -> None:
        conn = self._connected_conn()
        mock_cursor = MagicMock()
        conn._raw_conn.cursor.return_value = mock_cursor
        conn.execute("SELECT %s", ("hello",))
        mock_cursor.execute.assert_called_once_with("SELECT %s", ("hello",))

    def test_execute_wraps_snowflake_error(self) -> None:
        import snowflake.connector

        conn = self._connected_conn()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = snowflake.connector.Error("syntax error")
        conn._raw_conn.cursor.return_value = mock_cursor
        with pytest.raises(SnowforgeConnectionError, match="Query execution failed"):
            conn.execute("BAD SQL")


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_enter_calls_connect(self) -> None:
        conn = SnowforgeConnection(account="a", user="u", password="p")
        mock_raw = MagicMock()
        with patch("snowflake.connector.connect", return_value=mock_raw):
            with conn as c:
                assert c is conn
                assert conn._raw_conn is mock_raw

    def test_exit_calls_close(self) -> None:
        conn = SnowforgeConnection(account="a", user="u", password="p")
        mock_raw = MagicMock()
        with patch("snowflake.connector.connect", return_value=mock_raw):
            with conn:
                pass
        mock_raw.close.assert_called_once()
        assert conn._raw_conn is None

    def test_exit_closes_even_on_exception(self) -> None:
        conn = SnowforgeConnection(account="a", user="u", password="p")
        mock_raw = MagicMock()
        with patch("snowflake.connector.connect", return_value=mock_raw):
            with pytest.raises(ValueError):
                with conn:
                    raise ValueError("oops")
        mock_raw.close.assert_called_once()
