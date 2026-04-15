"""Connection helpers and context managers for Snowflake.

Provides a thin, typed wrapper around ``snowflake.connector.connect()`` that
supports both direct credential passing and environment variable resolution.
"""

from __future__ import annotations

import os
from types import TracebackType
from typing import Any

import snowflake.connector
import snowflake.connector.cursor

from snowcraft.exceptions import ConnectionError as SnowforgeConnectionError


class SnowforgeConnection:
    """A thin wrapper around ``snowflake.connector.connect()`` with env var support.

    All connection parameters fall back to the corresponding ``SNOWFLAKE_*``
    environment variable when not provided explicitly. Required parameters
    (``account``, ``user``, ``password``) are validated at construction time so
    failures are caught before any SQL reaches the warehouse.

    Credentials are never logged, even at DEBUG level.

    Args:
        account: Snowflake account identifier. Falls back to the
            ``SNOWFLAKE_ACCOUNT`` environment variable.
        user: Snowflake username. Falls back to ``SNOWFLAKE_USER``.
        password: Snowflake password. Falls back to ``SNOWFLAKE_PASSWORD``.
        database: Default database context. Falls back to ``SNOWFLAKE_DATABASE``.
        schema: Default schema context. Falls back to ``SNOWFLAKE_SCHEMA``.
        warehouse: Virtual warehouse to use. Falls back to
            ``SNOWFLAKE_WAREHOUSE``.
        role: Snowflake role. Falls back to ``SNOWFLAKE_ROLE``.

    Raises:
        ConnectionError: If any of ``account``, ``user``, or ``password`` cannot
            be resolved from arguments or environment variables.

    Example:
        with SnowforgeConnection() as conn:
            cursor = conn.execute("SELECT CURRENT_USER()")
            print(cursor.fetchone())
    """

    def __init__(
        self,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
    ) -> None:
        self._account = account or os.environ.get("SNOWFLAKE_ACCOUNT")
        self._user = user or os.environ.get("SNOWFLAKE_USER")
        self._password = password or os.environ.get("SNOWFLAKE_PASSWORD")
        self._database = database or os.environ.get("SNOWFLAKE_DATABASE")
        self._schema = schema or os.environ.get("SNOWFLAKE_SCHEMA")
        self._warehouse = warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE")
        self._role = role or os.environ.get("SNOWFLAKE_ROLE")

        self._validate_required_params()
        self._raw_conn: snowflake.connector.SnowflakeConnection | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_required_params(self) -> None:
        """Raise ConnectionError if any required parameter is missing."""
        missing: list[str] = []
        if not self._account:
            missing.append("account (or SNOWFLAKE_ACCOUNT)")
        if not self._user:
            missing.append("user (or SNOWFLAKE_USER)")
        if not self._password:
            missing.append("password (or SNOWFLAKE_PASSWORD)")
        if missing:
            raise SnowforgeConnectionError(
                "Missing required Snowflake connection parameters: " + ", ".join(missing)
            )

    def _build_connect_kwargs(self) -> dict[str, Any]:
        """Build the keyword arguments dict for snowflake.connector.connect()."""
        kwargs: dict[str, Any] = {
            "account": self._account,
            "user": self._user,
            "password": self._password,
        }
        if self._database:
            kwargs["database"] = self._database
        if self._schema:
            kwargs["schema"] = self._schema
        if self._warehouse:
            kwargs["warehouse"] = self._warehouse
        if self._role:
            kwargs["role"] = self._role
        return kwargs

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the Snowflake connection.

        Raises:
            ConnectionError: If the connector raises any error during connect.
        """
        try:
            self._raw_conn = snowflake.connector.connect(**self._build_connect_kwargs())
        except snowflake.connector.Error as exc:
            raise SnowforgeConnectionError(
                f"Failed to connect to Snowflake account '{self._account}': {exc}"
            ) from exc

    def close(self) -> None:
        """Close the Snowflake connection if it is open."""
        if self._raw_conn is not None:
            self._raw_conn.close()
            self._raw_conn = None

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def cursor(self) -> snowflake.connector.cursor.SnowflakeCursor:
        """Return a raw Snowflake cursor.

        Returns:
            A ``SnowflakeCursor`` instance tied to the current connection.

        Raises:
            ConnectionError: If the connection has not been opened yet.
        """
        if self._raw_conn is None:
            raise SnowforgeConnectionError(
                "Connection is not open. Use SnowforgeConnection as a context manager "
                "or call .connect() explicitly before calling .cursor()."
            )
        return self._raw_conn.cursor()

    def execute(
        self,
        sql: str,
        params: tuple[Any, ...] | None = None,
    ) -> snowflake.connector.cursor.SnowflakeCursor:
        """Execute a SQL statement and return the cursor.

        Args:
            sql: The SQL statement to execute.
            params: Optional positional parameters for a parameterized query.
                These are passed directly to the underlying cursor and are
                never interpolated via string formatting.

        Returns:
            The cursor after execution, ready to be fetched from.

        Raises:
            ConnectionError: If the connection is not open or the query fails.
        """
        cur = self.cursor()
        try:
            cur.execute(sql, params)
        except snowflake.connector.Error as exc:
            raise SnowforgeConnectionError(f"Query execution failed: {exc}") from exc
        return cur

    # ------------------------------------------------------------------
    # Context manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> SnowforgeConnection:
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
