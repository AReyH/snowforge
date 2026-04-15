"""Unit tests for snowcraft.scd.

Tests cover construction validation, SQL generation for Type 1 and Type 2
operations, and transaction/error handling.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from snowcraft.exceptions import MergeError
from snowcraft.scd import SCDManager, SCDResult

# ---------------------------------------------------------------------------
# SCDManager construction
# ---------------------------------------------------------------------------


class TestSCDManagerConstruction:
    def _make(self, mock_conn: MagicMock, **kwargs: object) -> SCDManager:
        defaults = dict(
            conn=mock_conn,
            target_table="MYDB.DW.DIM_CUSTOMER",
            source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
            business_keys=["customer_id"],
            tracked_columns=["name", "email"],
        )
        defaults.update(kwargs)
        return SCDManager(**defaults)  # type: ignore[arg-type]

    def test_valid_construction(self, mock_conn: MagicMock) -> None:
        mgr = self._make(mock_conn)
        assert mgr is not None

    def test_empty_business_keys_raises(self, mock_conn: MagicMock) -> None:
        with pytest.raises(MergeError, match="business_keys cannot be empty"):
            self._make(mock_conn, business_keys=[])

    def test_empty_tracked_columns_raises(self, mock_conn: MagicMock) -> None:
        with pytest.raises(MergeError, match="tracked_columns cannot be empty"):
            self._make(mock_conn, tracked_columns=[])

    def test_custom_scd2_column_names(self, mock_conn: MagicMock) -> None:
        mgr = self._make(
            mock_conn,
            effective_from_col="valid_from",
            effective_to_col="valid_to",
            current_flag_col="active",
        )
        assert mgr._effective_from_col == "valid_from"
        assert mgr._effective_to_col == "valid_to"
        assert mgr._current_flag_col == "active"


# ---------------------------------------------------------------------------
# SCDManager._build_expire_sql()
# ---------------------------------------------------------------------------


class TestBuildExpireSql:
    def _mgr(self, mock_conn: MagicMock) -> SCDManager:
        return SCDManager(
            conn=mock_conn,
            target_table="MYDB.DW.DIM_CUSTOMER",
            source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
            business_keys=["customer_id"],
            tracked_columns=["name", "email"],
        )

    def test_expire_sql_contains_update(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_expire_sql().upper()
        assert "UPDATE" in sql

    def test_expire_sql_targets_correct_table(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_expire_sql().upper()
        assert "DIM_CUSTOMER" in sql

    def test_expire_sql_sets_is_current_false(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_expire_sql()
        assert "FALSE" in sql.upper() or "false" in sql.lower()

    def test_expire_sql_sets_effective_to(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_expire_sql().upper()
        assert "EFFECTIVE_TO" in sql or "effective_to".upper() in sql

    def test_expire_sql_includes_all_tracked_columns(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_expire_sql()
        assert "name" in sql
        assert "email" in sql

    def test_expire_sql_filters_on_is_current(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_expire_sql().upper()
        assert "IS_CURRENT" in sql
        assert "TRUE" in sql


# ---------------------------------------------------------------------------
# SCDManager._build_insert_sql()
# ---------------------------------------------------------------------------


class TestBuildInsertSql:
    def _mgr(self, mock_conn: MagicMock) -> SCDManager:
        return SCDManager(
            conn=mock_conn,
            target_table="MYDB.DW.DIM_CUSTOMER",
            source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
            business_keys=["customer_id"],
            tracked_columns=["name", "email"],
        )

    def test_insert_sql_contains_insert_into(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_insert_sql(["customer_id", "name", "email"]).upper()
        assert "INSERT INTO" in sql

    def test_insert_sql_contains_open_end_date(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_insert_sql(["customer_id", "name", "email"])
        assert "9999-12-31" in sql

    def test_insert_sql_sets_is_current_true(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_insert_sql(["customer_id", "name", "email"]).upper()
        assert "TRUE" in sql

    def test_insert_sql_contains_effective_from(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_insert_sql(["customer_id", "name", "email"]).upper()
        assert "EFFECTIVE_FROM" in sql

    def test_insert_sql_left_joins_target(self, mock_conn: MagicMock) -> None:
        sql = self._mgr(mock_conn)._build_insert_sql(["customer_id", "name", "email"]).upper()
        assert "LEFT JOIN" in sql


# ---------------------------------------------------------------------------
# SCDManager.apply_type1()
# ---------------------------------------------------------------------------


class TestApplyType1:
    def test_delegates_to_merge_builder(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchone.return_value = (5, 3, 0)
        mgr = SCDManager(
            conn=mock_conn,
            target_table="MYDB.DW.DIM_CUSTOMER",
            source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
            business_keys=["customer_id"],
            tracked_columns=["name", "email"],
        )
        result = mgr.apply_type1()
        # Should have called execute at least for BEGIN + MERGE + COMMIT
        assert mock_conn.execute.call_count >= 3
        assert result.rows_inserted == 5
        assert result.rows_updated == 3


# ---------------------------------------------------------------------------
# SCDManager.apply_type2()
# ---------------------------------------------------------------------------


class TestApplyType2:
    def _mgr(self, mock_conn: MagicMock) -> SCDManager:
        return SCDManager(
            conn=mock_conn,
            target_table="MYDB.DW.DIM_CUSTOMER",
            source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
            business_keys=["customer_id"],
            tracked_columns=["name", "email"],
        )

    def test_apply_type2_returns_scd_result(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        # First fetchone is for the UPDATE (rows expired), second is for INSERT (rows inserted)
        mock_cursor.fetchone.side_effect = [(2,), (5,)]
        result = self._mgr(mock_conn).apply_type2()
        assert isinstance(result, SCDResult)
        assert result.rows_expired == 2
        assert result.rows_inserted == 5

    def test_apply_type2_wraps_in_transaction(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchone.side_effect = [(0,), (3,)]
        self._mgr(mock_conn).apply_type2()
        execute_sqls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("BEGIN" in s for s in execute_sqls)
        assert any("COMMIT" in s for s in execute_sqls)

    def test_apply_type2_rollback_on_failure(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = [
            MagicMock(),  # BEGIN
            RuntimeError("snowflake error"),  # UPDATE
        ]
        with pytest.raises(MergeError, match="SCD Type 2"):
            self._mgr(mock_conn).apply_type2()
        sqls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("ROLLBACK" in s for s in sqls)

    def test_star_select_raises(self, mock_conn: MagicMock) -> None:
        mgr = SCDManager(
            conn=mock_conn,
            target_table="MYDB.DW.DIM_CUSTOMER",
            source_query="SELECT * FROM staging",
            business_keys=["customer_id"],
            tracked_columns=["name"],
        )
        with pytest.raises(MergeError, match="explicit column list"):
            mgr.apply_type2()
