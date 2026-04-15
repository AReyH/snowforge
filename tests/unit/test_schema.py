"""Unit tests for snowcraft.schema.

Tests cover column metadata parsing, diff logic, the is_breaking flag,
and the Markdown / dict serialisation methods.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from snowcraft.exceptions import SchemaError
from snowcraft.schema import (
    ColumnDef,
    SchemaDiff,
    SchemaInspector,
    _extract_type_base_and_size,
    _is_type_narrowing,
    _parse_table_ref,
)

# ---------------------------------------------------------------------------
# _parse_table_ref
# ---------------------------------------------------------------------------


class TestParseTableRef:
    def test_three_parts(self) -> None:
        assert _parse_table_ref("MYDB.PUBLIC.ORDERS") == ("MYDB", "PUBLIC", "ORDERS")

    def test_lowercase_uppercased(self) -> None:
        db, schema, tbl = _parse_table_ref("mydb.public.orders")
        assert db == "MYDB"
        assert schema == "PUBLIC"
        assert tbl == "ORDERS"

    def test_quoted_parts_stripped(self) -> None:
        db, schema, tbl = _parse_table_ref('"MYDB"."PUBLIC"."ORDERS"')
        assert db == "MYDB"
        assert schema == "PUBLIC"
        assert tbl == "ORDERS"

    def test_one_part_raises(self) -> None:
        with pytest.raises(SchemaError, match="fully qualified"):
            _parse_table_ref("orders")

    def test_two_parts_raises(self) -> None:
        with pytest.raises(SchemaError, match="fully qualified"):
            _parse_table_ref("PUBLIC.ORDERS")


# ---------------------------------------------------------------------------
# _extract_type_base_and_size
# ---------------------------------------------------------------------------


class TestExtractTypeBaseAndSize:
    def test_plain_type(self) -> None:
        assert _extract_type_base_and_size("INTEGER") == ("INTEGER", None)

    def test_varchar_with_size(self) -> None:
        assert _extract_type_base_and_size("VARCHAR(256)") == ("VARCHAR", 256)

    def test_number_with_precision_scale(self) -> None:
        base, size = _extract_type_base_and_size("NUMBER(38,0)")
        assert base == "NUMBER"
        assert size == 38

    def test_lowercase_normalised(self) -> None:
        base, size = _extract_type_base_and_size("varchar(100)")
        assert base == "VARCHAR"
        assert size == 100


# ---------------------------------------------------------------------------
# _is_type_narrowing
# ---------------------------------------------------------------------------


class TestIsTypeNarrowing:
    def test_same_type_not_narrowing(self) -> None:
        assert not _is_type_narrowing("VARCHAR(256)", "VARCHAR(256)")

    def test_varchar_shorter_is_narrowing(self) -> None:
        assert _is_type_narrowing("VARCHAR(256)", "VARCHAR(64)")

    def test_varchar_longer_not_narrowing(self) -> None:
        assert not _is_type_narrowing("VARCHAR(64)", "VARCHAR(256)")

    def test_cross_type_is_narrowing(self) -> None:
        assert _is_type_narrowing("FLOAT", "INTEGER")

    def test_plain_types_same_not_narrowing(self) -> None:
        assert not _is_type_narrowing("INTEGER", "INTEGER")


# ---------------------------------------------------------------------------
# SchemaDiff.to_markdown()
# ---------------------------------------------------------------------------


class TestSchemaDiffToMarkdown:
    def _col(self, name: str, dtype: str = "VARCHAR(256)", nullable: bool = True) -> ColumnDef:
        return ColumnDef(
            name=name, data_type=dtype, is_nullable=nullable, default=None, comment=None
        )

    def test_empty_diff_message(self) -> None:
        diff = SchemaDiff()
        md = diff.to_markdown()
        assert "No schema differences" in md

    def test_added_columns_appear(self) -> None:
        diff = SchemaDiff(added=[self._col("new_col")])
        md = diff.to_markdown()
        assert "new_col" in md
        assert "Added" in md

    def test_removed_columns_appear(self) -> None:
        diff = SchemaDiff(removed=[self._col("old_col")], is_breaking=True)
        md = diff.to_markdown()
        assert "old_col" in md
        assert "Removed" in md

    def test_breaking_label_present(self) -> None:
        diff = SchemaDiff(removed=[self._col("gone")], is_breaking=True)
        md = diff.to_markdown()
        assert "BREAKING" in md

    def test_type_changed_appears(self) -> None:
        old = self._col("amount", dtype="VARCHAR(256)")
        new = self._col("amount", dtype="VARCHAR(64)")
        diff = SchemaDiff(type_changed=[(old, new)], is_breaking=True)
        md = diff.to_markdown()
        assert "amount" in md
        assert "Type changes" in md


# ---------------------------------------------------------------------------
# SchemaDiff.to_dict()
# ---------------------------------------------------------------------------


class TestSchemaDiffToDict:
    def _col(self, name: str) -> ColumnDef:
        return ColumnDef(name=name, data_type="TEXT", is_nullable=True, default=None, comment=None)

    def test_empty_diff_dict_structure(self) -> None:
        d = SchemaDiff().to_dict()
        assert d["added"] == []
        assert d["removed"] == []
        assert d["type_changed"] == []
        assert d["nullability_changed"] == []
        assert d["is_breaking"] is False

    def test_added_column_in_dict(self) -> None:
        d = SchemaDiff(added=[self._col("new_col")]).to_dict()
        assert len(d["added"]) == 1
        assert d["added"][0]["name"] == "new_col"


# ---------------------------------------------------------------------------
# SchemaInspector.get_columns()
# ---------------------------------------------------------------------------


class TestSchemaInspectorGetColumns:
    def _make_row(
        self,
        name: str = "id",
        dtype: str = "TEXT",
        char_len: object = None,
        num_prec: object = None,
        num_scale: object = None,
        nullable: str = "YES",
        default: object = None,
        comment: object = None,
    ) -> tuple[object, ...]:
        return (name, dtype, char_len, num_prec, num_scale, nullable, default, comment)

    def test_get_columns_returns_column_defs(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchall.return_value = [
            self._make_row("order_id", "TEXT", None, None, None, "NO"),
            self._make_row("status", "TEXT", 256, None, None, "YES"),
        ]
        inspector = SchemaInspector(mock_conn)
        cols = inspector.get_columns("MYDB.PUBLIC.ORDERS")
        assert len(cols) == 2
        assert cols[0].name == "order_id"
        assert not cols[0].is_nullable
        assert cols[1].is_nullable

    def test_varchar_size_included_in_type(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchall.return_value = [
            self._make_row("name", "TEXT", 256, None, None, "YES"),
        ]
        inspector = SchemaInspector(mock_conn)
        cols = inspector.get_columns("MYDB.PUBLIC.CUSTOMERS")
        assert "256" in cols[0].data_type

    def test_invalid_table_ref_raises(self, mock_conn: MagicMock) -> None:
        inspector = SchemaInspector(mock_conn)
        with pytest.raises(SchemaError, match="fully qualified"):
            inspector.get_columns("just_table")

    def test_query_failure_raises_schema_error(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = RuntimeError("permission denied")
        inspector = SchemaInspector(mock_conn)
        with pytest.raises(SchemaError, match="Failed to fetch"):
            inspector.get_columns("MYDB.PUBLIC.ORDERS")


# ---------------------------------------------------------------------------
# SchemaInspector.diff()
# ---------------------------------------------------------------------------


class TestSchemaInspectorDiff:
    def _col(
        self,
        name: str,
        dtype: str = "TEXT",
        nullable: bool = True,
    ) -> ColumnDef:
        return ColumnDef(
            name=name, data_type=dtype, is_nullable=nullable, default=None, comment=None
        )

    def test_no_change_returns_empty_diff(self, mock_conn: MagicMock) -> None:
        cols = [self._col("id"), self._col("name")]
        inspector = SchemaInspector(mock_conn)
        inspector.get_columns = MagicMock(return_value=cols)  # type: ignore[method-assign]
        diff = inspector.diff("DB.S.SOURCE", "DB.S.TARGET")
        assert not diff.added
        assert not diff.removed
        assert not diff.is_breaking

    def test_added_column_detected(self, mock_conn: MagicMock) -> None:
        source_cols = [self._col("id"), self._col("email")]
        target_cols = [self._col("id")]
        inspector = SchemaInspector(mock_conn)
        inspector.get_columns = MagicMock(side_effect=[source_cols, target_cols])  # type: ignore[method-assign]
        diff = inspector.diff("DB.S.SOURCE", "DB.S.TARGET")
        assert len(diff.added) == 1
        assert diff.added[0].name == "email"
        assert not diff.is_breaking

    def test_removed_column_is_breaking(self, mock_conn: MagicMock) -> None:
        source_cols = [self._col("id")]
        target_cols = [self._col("id"), self._col("old_col")]
        inspector = SchemaInspector(mock_conn)
        inspector.get_columns = MagicMock(side_effect=[source_cols, target_cols])  # type: ignore[method-assign]
        diff = inspector.diff("DB.S.SOURCE", "DB.S.TARGET")
        assert len(diff.removed) == 1
        assert diff.is_breaking

    def test_type_narrowing_is_breaking(self, mock_conn: MagicMock) -> None:
        source_cols = [self._col("name", dtype="VARCHAR(64)")]
        target_cols = [self._col("name", dtype="VARCHAR(256)")]
        inspector = SchemaInspector(mock_conn)
        inspector.get_columns = MagicMock(side_effect=[source_cols, target_cols])  # type: ignore[method-assign]
        diff = inspector.diff("DB.S.SOURCE", "DB.S.TARGET")
        assert len(diff.type_changed) == 1
        assert diff.is_breaking

    def test_nullable_to_not_nullable_is_breaking(self, mock_conn: MagicMock) -> None:
        source_cols = [self._col("name", nullable=False)]
        target_cols = [self._col("name", nullable=True)]
        inspector = SchemaInspector(mock_conn)
        inspector.get_columns = MagicMock(side_effect=[source_cols, target_cols])  # type: ignore[method-assign]
        diff = inspector.diff("DB.S.SOURCE", "DB.S.TARGET")
        assert len(diff.nullability_changed) == 1
        assert diff.is_breaking
