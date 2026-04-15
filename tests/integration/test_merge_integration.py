"""Integration tests for snowcraft.merge.

These tests require a live Snowflake connection and are automatically skipped
when the ``SNOWFLAKE_ACCOUNT`` environment variable is not set.

Never run these against a production account. Use a dedicated test account with
the ``SNOWCRAFT_TEST`` database.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest

from snowcraft.connection import SnowcraftConnection
from snowcraft.merge import MergeBuilder, MergeResult

_TARGET = "SNOWCRAFT_TEST.PUBLIC.ORDERS_TARGET"
_STAGING = "SNOWCRAFT_TEST.PUBLIC.ORDERS_STAGING"
_SOURCE_QUERY = f"SELECT order_id, status, updated_at FROM {_STAGING}"
_SEED_ROW = f"INSERT INTO {_TARGET} VALUES (1, 'pending', '2024-01-01 00:00:00')"


@pytest.mark.usefixtures("integration_database")
class TestMergeIntegration:
    """End-to-end MERGE tests against a real Snowflake environment."""

    @pytest.fixture(autouse=True)
    def setup_tables(self, integration_conn: SnowcraftConnection) -> Generator[None, None, None]:
        """Create staging and target tables; drop them after each test."""
        integration_conn.execute(
            f"""
            CREATE OR REPLACE TABLE {_TARGET} (
                order_id     NUMBER    NOT NULL,
                status       VARCHAR   NOT NULL,
                updated_at   TIMESTAMP NOT NULL,
                PRIMARY KEY (order_id)
            )
            """
        )
        integration_conn.execute(
            f"""
            CREATE OR REPLACE TABLE {_STAGING} (
                order_id     NUMBER    NOT NULL,
                status       VARCHAR   NOT NULL,
                updated_at   TIMESTAMP NOT NULL
            )
            """
        )
        yield
        integration_conn.execute(f"DROP TABLE IF EXISTS {_TARGET}")
        integration_conn.execute(f"DROP TABLE IF EXISTS {_STAGING}")

    def _seed_staging(self, conn: SnowcraftConnection, rows: list[tuple[int, str, str]]) -> None:
        for order_id, status, ts in rows:
            conn.execute(
                f"INSERT INTO {_STAGING} VALUES (%s, %s, %s)",
                (order_id, status, ts),
            )

    def test_upsert_inserts_new_rows(self, integration_conn: SnowcraftConnection) -> None:
        self._seed_staging(
            integration_conn,
            [(1, "pending", "2024-01-01 00:00:00"), (2, "shipped", "2024-01-02 00:00:00")],
        )

        result = MergeBuilder(
            conn=integration_conn,
            target_table=_TARGET,
            source_query=_SOURCE_QUERY,
            match_keys=["order_id"],
        ).execute()

        assert isinstance(result, MergeResult)
        assert result.rows_inserted == 2
        assert result.rows_updated == 0

    def test_upsert_updates_existing_rows(self, integration_conn: SnowcraftConnection) -> None:
        integration_conn.execute(_SEED_ROW)
        self._seed_staging(integration_conn, [(1, "shipped", "2024-01-02 00:00:00")])

        result = MergeBuilder(
            conn=integration_conn,
            target_table=_TARGET,
            source_query=_SOURCE_QUERY,
            match_keys=["order_id"],
        ).execute()

        assert result.rows_inserted == 0
        assert result.rows_updated == 1

        cur = integration_conn.execute(f"SELECT status FROM {_TARGET} WHERE order_id = 1")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == "shipped"

    def test_append_strategy_skips_existing(self, integration_conn: SnowcraftConnection) -> None:
        integration_conn.execute(_SEED_ROW)
        self._seed_staging(
            integration_conn,
            [(1, "shipped", "2024-01-02 00:00:00"), (2, "pending", "2024-01-01 00:00:00")],
        )

        result = MergeBuilder(
            conn=integration_conn,
            target_table=_TARGET,
            source_query=_SOURCE_QUERY,
            match_keys=["order_id"],
            strategy="append",
        ).execute()

        assert result.rows_inserted == 1  # only order_id=2 is new
        assert result.rows_updated == 0

    def test_build_returns_valid_sql(self, integration_conn: SnowcraftConnection) -> None:
        builder = MergeBuilder(
            conn=integration_conn,
            target_table=_TARGET,
            source_query=_SOURCE_QUERY,
            match_keys=["order_id"],
        )
        sql = builder.build()
        assert "MERGE" in sql.upper()
        assert "SNOWCRAFT_TEST" in sql.upper()
