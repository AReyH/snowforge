"""Unit tests for snowcraft.merge.

All tests mock the Snowflake connection and cursor — no real warehouse is used.
Tests focus on SQL generation correctness, parameter validation, and result
parsing.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from snowcraft.exceptions import MergeError
from snowcraft.merge import MergeBuilder, MergeResult, _extract_select_columns, _inject_watermark

# ---------------------------------------------------------------------------
# _extract_select_columns
# ---------------------------------------------------------------------------


class TestExtractSelectColumns:
    def test_explicit_columns(self) -> None:
        cols = _extract_select_columns("SELECT order_id, status, updated_at FROM staging.orders")
        assert cols == ["order_id", "status", "updated_at"]

    def test_aliased_columns(self) -> None:
        cols = _extract_select_columns("SELECT id AS order_id, UPPER(status) AS status FROM t")
        assert cols == ["order_id", "status"]

    def test_star_returns_empty(self) -> None:
        assert _extract_select_columns("SELECT * FROM orders") == []

    def test_invalid_sql_returns_empty(self) -> None:
        assert _extract_select_columns("THIS IS NOT SQL") == []

    def test_non_select_returns_empty(self) -> None:
        assert _extract_select_columns("INSERT INTO t VALUES (1)") == []


# ---------------------------------------------------------------------------
# _inject_watermark
# ---------------------------------------------------------------------------


class TestInjectWatermark:
    def test_adds_where_clause(self) -> None:
        sql = "SELECT id, ts FROM staging.events"
        result = _inject_watermark(sql, "ts", "2024-01-01 00:00:00")
        assert "WHERE" in result.upper()
        assert "ts" in result
        assert "2024-01-01 00:00:00" in result

    def test_appends_to_existing_where(self) -> None:
        sql = "SELECT id, ts FROM staging.events WHERE region = 'US'"
        result = _inject_watermark(sql, "ts", "2024-01-01")
        upper = result.upper()
        assert upper.count("WHERE") == 1
        assert "AND" in upper

    def test_invalid_sql_raises(self) -> None:
        with pytest.raises(MergeError, match="Could not parse source_query"):
            _inject_watermark("NOT SQL AT ALL !!!", "ts", "2024-01-01")

    def test_non_select_raises(self) -> None:
        with pytest.raises(MergeError, match="must be a SELECT statement"):
            _inject_watermark("INSERT INTO t VALUES (1)", "ts", "2024-01-01")


# ---------------------------------------------------------------------------
# MergeBuilder — construction validation
# ---------------------------------------------------------------------------


class TestMergeBuilderConstruction:
    def test_empty_match_keys_raises(self, mock_conn: MagicMock) -> None:
        with pytest.raises(MergeError, match="match_keys cannot be empty"):
            MergeBuilder(
                conn=mock_conn,
                target_table="DB.S.T",
                source_query="SELECT a, b FROM src",
                match_keys=[],
            )

    def test_star_select_without_update_columns_raises(self, mock_conn: MagicMock) -> None:
        with pytest.raises(MergeError, match="SELECT \\*"):
            MergeBuilder(
                conn=mock_conn,
                target_table="DB.S.T",
                source_query="SELECT * FROM src",
                match_keys=["id"],
            )

    def test_star_select_with_update_columns_ok(self, mock_conn: MagicMock) -> None:
        # Should not raise
        MergeBuilder(
            conn=mock_conn,
            target_table="DB.S.T",
            source_query="SELECT * FROM src",
            match_keys=["id"],
            update_columns=["name", "status"],
        )

    def test_star_select_append_strategy_ok(self, mock_conn: MagicMock) -> None:
        # append strategy never updates, so no column list needed
        MergeBuilder(
            conn=mock_conn,
            target_table="DB.S.T",
            source_query="SELECT * FROM src",
            match_keys=["id"],
            strategy="append",
        )


# ---------------------------------------------------------------------------
# MergeBuilder.build()
# ---------------------------------------------------------------------------


class TestMergeBuilderBuild:
    def _builder(self, mock_conn: MagicMock, **kwargs: object) -> MergeBuilder:
        defaults = dict(
            conn=mock_conn,
            target_table="MYDB.PUBLIC.ORDERS",
            source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
            match_keys=["order_id"],
        )
        defaults.update(kwargs)
        return MergeBuilder(**defaults)  # type: ignore[arg-type]

    def test_build_returns_string(self, mock_conn: MagicMock) -> None:
        sql = self._builder(mock_conn).build()
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_upsert_contains_merge_into(self, mock_conn: MagicMock) -> None:
        sql = self._builder(mock_conn).build().upper()
        assert "MERGE INTO" in sql

    def test_upsert_contains_when_matched_update(self, mock_conn: MagicMock) -> None:
        sql = self._builder(mock_conn).build().upper()
        assert "WHEN MATCHED" in sql
        assert "UPDATE" in sql

    def test_upsert_contains_when_not_matched_insert(self, mock_conn: MagicMock) -> None:
        sql = self._builder(mock_conn).build().upper()
        assert "WHEN NOT MATCHED" in sql
        assert "INSERT" in sql

    def test_append_strategy_no_update_clause(self, mock_conn: MagicMock) -> None:
        sql = self._builder(mock_conn, strategy="append").build().upper()
        assert "WHEN NOT MATCHED" in sql
        # append should NOT have a WHEN MATCHED clause
        assert "WHEN MATCHED" not in sql

    def test_target_table_appears_in_sql(self, mock_conn: MagicMock) -> None:
        sql = self._builder(mock_conn).build()
        # The table name should appear (possibly quoted)
        assert "ORDERS" in sql.upper()

    def test_match_keys_appear_in_on_clause(self, mock_conn: MagicMock) -> None:
        sql = self._builder(mock_conn).build()
        assert "order_id" in sql

    def test_watermark_injected_into_sql(self, mock_conn: MagicMock) -> None:
        builder = self._builder(mock_conn, watermark_column="updated_at")
        sql = builder.build(watermark_value="2024-06-01 00:00:00")
        assert "2024-06-01 00:00:00" in sql

    def test_build_does_not_require_connection(self, mock_conn: MagicMock) -> None:
        builder = self._builder(mock_conn)
        # build() should not call conn.execute at all
        builder.build()
        mock_conn.execute.assert_not_called()

    def test_multi_key_join_condition(self, mock_conn: MagicMock) -> None:
        builder = self._builder(
            mock_conn,
            source_query="SELECT region, order_id, status FROM src",
            match_keys=["region", "order_id"],
        )
        sql = builder.build().upper()
        assert "AND" in sql  # two keys joined with AND

    def test_explicit_update_columns(self, mock_conn: MagicMock) -> None:
        builder = MergeBuilder(
            conn=mock_conn,
            target_table="DB.S.T",
            source_query="SELECT * FROM src",
            match_keys=["id"],
            update_columns=["name"],
        )
        sql = builder.build()
        assert "name" in sql

    def test_invalid_source_query_raises(self, mock_conn: MagicMock) -> None:
        builder = MergeBuilder(
            conn=mock_conn,
            target_table="DB.S.T",
            source_query="SELECT a, b FROM src",  # valid initially
            match_keys=["a"],
        )
        # Patch the source query to invalid SQL after construction
        builder._source_query = "COMPLETELY INVALID SQL"
        with pytest.raises(MergeError, match="Could not parse source_query"):
            builder.build()


# ---------------------------------------------------------------------------
# MergeBuilder.execute()
# ---------------------------------------------------------------------------


class TestMergeBuilderExecute:
    def _builder(self, mock_conn: MagicMock, **kwargs: object) -> MergeBuilder:
        defaults = dict(
            conn=mock_conn,
            target_table="MYDB.PUBLIC.ORDERS",
            source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
            match_keys=["order_id"],
        )
        defaults.update(kwargs)
        return MergeBuilder(**defaults)  # type: ignore[arg-type]

    def test_execute_returns_merge_result(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchone.return_value = (10, 5, 0)
        result = self._builder(mock_conn).execute()
        assert isinstance(result, MergeResult)

    def test_execute_parses_row_counts(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchone.return_value = (7, 3, 1)
        result = self._builder(mock_conn).execute()
        assert result.rows_inserted == 7
        assert result.rows_updated == 3
        assert result.rows_deleted == 1

    def test_execute_handles_none_fetchone(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchone.return_value = None
        result = self._builder(mock_conn).execute()
        assert result.rows_inserted == 0
        assert result.rows_updated == 0
        assert result.rows_deleted == 0

    def test_execute_commits_transaction(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchone.return_value = (0, 0, 0)
        self._builder(mock_conn).execute()
        execute_calls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("BEGIN" in c for c in execute_calls)
        assert any("COMMIT" in c for c in execute_calls)

    def test_execute_rolls_back_on_failure(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = [
            MagicMock(),  # BEGIN
            RuntimeError("Snowflake error"),  # the MERGE itself
        ]
        with pytest.raises(MergeError, match="MERGE execution failed"):
            self._builder(mock_conn).execute()

        execute_calls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("ROLLBACK" in c for c in execute_calls)

    def test_execute_stores_query_id(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.sfqid = "abc-123"
        mock_cursor.fetchone.return_value = (1, 0, 0)
        result = self._builder(mock_conn).execute()
        assert result.query_id == "abc-123"

    def test_execute_tracks_time(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchone.return_value = (0, 0, 0)
        result = self._builder(mock_conn).execute()
        assert result.execution_time_ms >= 0

    def test_rollback_failure_still_raises_merge_error(self, mock_conn: MagicMock) -> None:
        """ROLLBACK itself blowing up should not hide the original MergeError."""
        call_count = 0

        def side_effect(*_args: object, **_kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MagicMock()  # BEGIN
            if call_count == 2:
                raise RuntimeError("MERGE failed")  # the MERGE itself
            raise RuntimeError("ROLLBACK failed")  # ROLLBACK also fails

        mock_conn.execute.side_effect = side_effect
        with pytest.raises(MergeError, match="MERGE execution failed"):
            self._builder(mock_conn).execute()

    def test_watermark_read_failure_raises(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = RuntimeError("permission denied")
        builder = MergeBuilder(
            conn=mock_conn,
            target_table="DB.S.T",
            source_query="SELECT a, b FROM src",
            match_keys=["a"],
            watermark_column="b",
            watermark_table="DB.S.WATERMARKS",
        )
        with pytest.raises(MergeError, match="Failed to read watermark"):
            builder.execute()

    def test_watermark_execute_calls_update(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchone.return_value = (1, 0, 0)
        builder = MergeBuilder(
            conn=mock_conn,
            target_table="DB.S.T",
            source_query="SELECT a, b FROM src",
            match_keys=["a"],
            watermark_column="b",
            watermark_table="DB.S.WATERMARKS",
        )
        builder.execute()
        # watermark read + BEGIN + MERGE + watermark update + COMMIT = 5 calls minimum
        assert mock_conn.execute.call_count >= 5


class TestMergeBuilderDeleteInsertStrategy:
    def test_delete_insert_contains_delete(self, mock_conn: MagicMock) -> None:
        builder = MergeBuilder(
            conn=mock_conn,
            target_table="DB.S.T",
            source_query="SELECT id, val FROM src",
            match_keys=["id"],
            strategy="delete_insert",
        )
        sql = builder.build().upper()
        assert "DELETE" in sql
        assert "WHEN NOT MATCHED" in sql


class TestExtractSelectColumnsEdgeCases:
    def test_expression_with_name_attr(self) -> None:
        # A function call like CURRENT_TIMESTAMP() aliased
        cols = _extract_select_columns("SELECT CURRENT_TIMESTAMP() AS ts FROM t")
        assert "ts" in cols

    def test_parse_error_returns_empty(self) -> None:
        # Ensure parse errors return empty, not raise
        result = _extract_select_columns("SELECT FROM WHERE AND")
        assert result == []
