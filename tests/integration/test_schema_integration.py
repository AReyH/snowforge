"""Integration tests for snowcraft.schema.

These tests require a live Snowflake connection and are automatically skipped
when the ``SNOWFLAKE_ACCOUNT`` environment variable is not set.
"""

from __future__ import annotations

import pytest

from snowcraft.connection import SnowforgeConnection
from snowcraft.schema import SchemaDiff, SchemaInspector


@pytest.mark.usefixtures("integration_database")
class TestSchemaInspectorIntegration:
    @pytest.fixture(autouse=True)
    def setup_tables(self, integration_conn: SnowforgeConnection) -> None:
        """Create two tables with different schemas for diff testing."""
        integration_conn.execute(
            """
            CREATE OR REPLACE TABLE SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE (
                id          NUMBER    NOT NULL COMMENT 'Primary key',
                name        VARCHAR(256),
                email       VARCHAR(512),
                created_at  TIMESTAMP NOT NULL
            )
            """
        )
        integration_conn.execute(
            """
            CREATE OR REPLACE TABLE SNOWFORGE_TEST.PUBLIC.SCHEMA_TARGET (
                id          NUMBER    NOT NULL COMMENT 'Primary key',
                name        VARCHAR(64),   -- narrower than source
                legacy_col  VARCHAR        -- will show as removed
            )
            """
        )
        yield
        integration_conn.execute("DROP TABLE IF EXISTS SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE")
        integration_conn.execute("DROP TABLE IF EXISTS SNOWFORGE_TEST.PUBLIC.SCHEMA_TARGET")

    def test_get_columns_returns_correct_count(self, integration_conn: SnowforgeConnection) -> None:
        inspector = SchemaInspector(integration_conn)
        cols = inspector.get_columns("SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE")
        assert len(cols) == 4

    def test_get_columns_preserves_order(self, integration_conn: SnowforgeConnection) -> None:
        inspector = SchemaInspector(integration_conn)
        cols = inspector.get_columns("SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE")
        assert cols[0].name.upper() == "ID"

    def test_get_columns_comment_preserved(self, integration_conn: SnowforgeConnection) -> None:
        inspector = SchemaInspector(integration_conn)
        cols = inspector.get_columns("SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE")
        id_col = next(c for c in cols if c.name.upper() == "ID")
        assert id_col.comment == "Primary key"

    def test_diff_detects_added_columns(self, integration_conn: SnowforgeConnection) -> None:
        inspector = SchemaInspector(integration_conn)
        diff: SchemaDiff = inspector.diff(
            source="SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE",
            target="SNOWFORGE_TEST.PUBLIC.SCHEMA_TARGET",
        )
        added_names = {c.name.upper() for c in diff.added}
        assert "EMAIL" in added_names
        assert "CREATED_AT" in added_names

    def test_diff_detects_removed_columns(self, integration_conn: SnowforgeConnection) -> None:
        inspector = SchemaInspector(integration_conn)
        diff = inspector.diff(
            source="SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE",
            target="SNOWFORGE_TEST.PUBLIC.SCHEMA_TARGET",
        )
        removed_names = {c.name.upper() for c in diff.removed}
        assert "LEGACY_COL" in removed_names

    def test_diff_is_breaking_due_to_removal(self, integration_conn: SnowforgeConnection) -> None:
        inspector = SchemaInspector(integration_conn)
        diff = inspector.diff(
            source="SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE",
            target="SNOWFORGE_TEST.PUBLIC.SCHEMA_TARGET",
        )
        assert diff.is_breaking

    def test_diff_to_markdown_non_empty(self, integration_conn: SnowforgeConnection) -> None:
        inspector = SchemaInspector(integration_conn)
        diff = inspector.diff(
            source="SNOWFORGE_TEST.PUBLIC.SCHEMA_SOURCE",
            target="SNOWFORGE_TEST.PUBLIC.SCHEMA_TARGET",
        )
        md = diff.to_markdown()
        assert len(md) > 50
        assert "#" in md  # has Markdown headers
