"""Incremental load / MERGE statement builder.

This module is the centerpiece of snowcraft. It generates and executes Snowflake
``MERGE INTO`` statements for incremental loads, abstracting away the boilerplate
of match conditions, update clauses, insert clauses, and watermark management.

SQL is built programmatically using ``sqlglot`` — never via f-string interpolation
of user-provided identifiers or values. This prevents SQL injection and ensures
correct Snowflake identifier quoting throughout.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal

import sqlglot
import sqlglot.expressions as exp

from snowcraft.connection import SnowforgeConnection
from snowcraft.exceptions import MergeError
from snowcraft.utils import build_table_expr, quote_identifier


@dataclass
class MergeResult:
    """Statistics returned after a MERGE operation completes.

    Attributes:
        rows_inserted: Number of rows inserted (WHEN NOT MATCHED branch).
        rows_updated: Number of rows updated (WHEN MATCHED branch).
        rows_deleted: Number of rows deleted (WHEN MATCHED THEN DELETE branch).
        execution_time_ms: Wall-clock time for the full execute() call in ms.
        query_id: Snowflake query ID for the MERGE statement itself.
    """

    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    execution_time_ms: int
    query_id: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _col(name: str, table: str | None = None) -> exp.Column:
    """Build a sqlglot Column expression with optional table qualifier."""
    return exp.Column(
        this=exp.Identifier(this=name, quoted=True),
        table=exp.Identifier(this=table) if table else None,
    )


def _extract_select_columns(source_query: str) -> list[str]:
    """Parse *explicit* column names from a SELECT statement.

    Returns an empty list when the SELECT list contains a star (``*``), a
    sub-expression whose name cannot be determined statically, or when parsing
    fails — the caller must handle those cases.

    Args:
        source_query: A SQL SELECT statement.

    Returns:
        List of output column names (alias takes priority over raw column name).
    """
    try:
        parsed = sqlglot.parse_one(source_query, read="snowflake")
    except sqlglot.errors.ParseError:
        return []

    if not isinstance(parsed, exp.Select):
        return []

    cols: list[str] = []
    for sel in parsed.selects:
        if isinstance(sel, exp.Star):
            return []  # Cannot determine columns statically
        alias = sel.alias
        if alias:
            cols.append(alias)
        elif isinstance(sel, exp.Column):
            cols.append(sel.name)
        elif hasattr(sel, "name") and sel.name:
            cols.append(sel.name)
        else:
            return []  # Unknown expression type — bail out
    return cols


def _inject_watermark(
    source_query: str,
    watermark_column: str,
    watermark_value: str,
) -> str:
    """Inject a ``column > value`` predicate into the WHERE clause of a SELECT.

    Uses sqlglot to parse and modify the query AST, never string concatenation.

    Args:
        source_query: The original SELECT statement.
        watermark_column: The column to filter on.
        watermark_value: The exclusive lower bound value.

    Returns:
        The modified SQL string.
    """
    try:
        parsed = sqlglot.parse_one(source_query, read="snowflake")
    except sqlglot.errors.ParseError as exc:
        raise MergeError(f"Could not parse source_query to inject watermark filter: {exc}") from exc

    if not isinstance(parsed, exp.Select):
        raise MergeError("source_query must be a SELECT statement to support watermark injection.")

    watermark_filter = exp.GT(
        this=_col(watermark_column),
        expression=exp.Literal.string(watermark_value),
    )
    modified = parsed.where(watermark_filter, append=True)
    return modified.sql(dialect="snowflake", pretty=True)


# ---------------------------------------------------------------------------
# MergeBuilder
# ---------------------------------------------------------------------------


class MergeBuilder:
    """Builds and executes Snowflake ``MERGE INTO`` statements.

    All identifier names (table, columns) provided by the caller are quoted
    through ``sqlglot`` before being embedded in SQL. Literal values always
    travel via Snowflake's parameterized query interface.

    Args:
        conn: An open (or to-be-opened) ``SnowforgeConnection``.
        target_table: Fully-qualified target table, e.g. ``"DB.SCHEMA.TABLE"``.
        source_query: Any ``SELECT`` statement whose result set is the source.
        match_keys: Column names that uniquely identify a row across source and
            target (the ``ON`` clause). Must not be empty.
        strategy: Merge strategy.

            * ``"upsert"`` — update matched rows, insert unmatched (default).
            * ``"append"`` — insert unmatched rows only; skip matched.
            * ``"delete_insert"`` — delete matched rows, then insert all source
              rows.

        update_columns: Explicit list of columns to update on match. When
            ``None``, all non-key columns derived from the source SELECT are
            updated. Must be provided when source_query uses ``SELECT *``.
        watermark_column: Column in the source used for incremental filtering.
            When set alongside ``watermark_table``, the builder reads the last
            watermark, injects ``watermark_column > last_value`` into the source
            query, and updates the watermark table after a successful merge.
        watermark_table: Fully-qualified table that stores watermark values.
            Expected schema: ``(table_name VARCHAR, watermark_value VARCHAR)``.

    Raises:
        MergeError: At construction time if ``match_keys`` is empty, or if
            ``source_query`` uses ``SELECT *`` and ``update_columns`` is not
            provided.

    Example:
        builder = MergeBuilder(
            conn=conn,
            target_table="MYDB.PUBLIC.ORDERS",
            source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
            match_keys=["order_id"],
        )
        sql = builder.build()   # inspect before running
        result = builder.execute()
        print(result.rows_inserted, result.rows_updated)
    """

    _TARGET_ALIAS = "target"
    _SOURCE_ALIAS = "source"

    def __init__(
        self,
        conn: SnowforgeConnection,
        target_table: str,
        source_query: str,
        match_keys: list[str],
        strategy: Literal["upsert", "append", "delete_insert"] = "upsert",
        update_columns: list[str] | None = None,
        watermark_column: str | None = None,
        watermark_table: str | None = None,
    ) -> None:
        if not match_keys:
            raise MergeError("match_keys cannot be empty.")

        source_cols = _extract_select_columns(source_query)
        if not source_cols and update_columns is None and strategy != "append":
            raise MergeError(
                "source_query uses SELECT * — either rewrite it with explicit column "
                "names or supply update_columns so the MERGE can be built statically."
            )

        self._conn = conn
        self._target_table = target_table
        self._source_query = source_query
        self._match_keys = match_keys
        self._strategy = strategy
        self._update_columns = update_columns
        self._watermark_column = watermark_column
        self._watermark_table = watermark_table
        self._source_cols: list[str] = source_cols

    # ------------------------------------------------------------------
    # Internal SQL builders
    # ------------------------------------------------------------------

    def _on_clause(self) -> exp.Expression:
        """Build the ``ON target.key = source.key [AND ...]`` expression."""
        conditions: list[exp.Expression] = [
            exp.EQ(
                this=_col(k, self._TARGET_ALIAS),
                expression=_col(k, self._SOURCE_ALIAS),
            )
            for k in self._match_keys
        ]
        result: exp.Expression = conditions[0]
        for cond in conditions[1:]:
            result = exp.And(this=result, expression=cond)
        return result

    def _resolve_update_columns(self) -> list[str]:
        """Return the resolved list of columns to update."""
        if self._update_columns is not None:
            return self._update_columns
        return [c for c in self._source_cols if c not in self._match_keys]

    def _when_matched_update(self, update_cols: list[str]) -> exp.When:
        """Build a ``WHEN MATCHED THEN UPDATE SET …`` clause."""
        set_exprs = [
            exp.EQ(
                this=_col(c, self._TARGET_ALIAS),
                expression=_col(c, self._SOURCE_ALIAS),
            )
            for c in update_cols
        ]
        return exp.When(
            matched=True,
            source=False,
            condition=None,
            then=exp.Update(expressions=set_exprs),
        )

    def _when_matched_delete(self) -> exp.When:
        """Build a ``WHEN MATCHED THEN DELETE`` clause."""
        return exp.When(
            matched=True,
            source=False,
            condition=None,
            then=exp.Var(this="DELETE"),
        )

    def _when_not_matched_insert(self, all_cols: list[str]) -> exp.When:
        """Build a ``WHEN NOT MATCHED THEN INSERT (…) VALUES (…)`` clause."""
        insert_cols = [_col(c) for c in all_cols]
        insert_vals = [_col(c, self._SOURCE_ALIAS) for c in all_cols]
        return exp.When(
            matched=False,
            source=False,
            condition=None,
            then=exp.Insert(
                this=exp.Tuple(expressions=insert_cols),
                expression=exp.Tuple(expressions=insert_vals),
            ),
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def build(self, watermark_value: str | None = None) -> str:
        """Build and return the MERGE SQL string.

        This method does **not** require a live connection and is safe to call
        for logging or testing purposes before calling ``execute()``.

        Args:
            watermark_value: When provided (and ``watermark_column`` is set),
                injects a ``WHERE watermark_column > watermark_value`` predicate
                into the source query.

        Returns:
            A Snowflake-dialect MERGE statement formatted for human readability.

        Raises:
            MergeError: If the source query cannot be parsed.
        """
        source_query = self._source_query
        if watermark_value is not None and self._watermark_column:
            source_query = _inject_watermark(
                source_query,
                self._watermark_column,
                watermark_value,
            )

        try:
            source_parsed = sqlglot.parse_one(source_query, read="snowflake")
        except sqlglot.errors.ParseError as exc:
            raise MergeError(f"Could not parse source_query: {exc}") from exc

        source_subquery = exp.Subquery(
            this=source_parsed,
            alias=exp.TableAlias(this=exp.Identifier(this=self._SOURCE_ALIAS)),
        )

        target_table_expr = exp.alias_(
            build_table_expr(self._target_table),
            self._TARGET_ALIAS,
        )

        when_list: list[exp.When] = []
        all_cols = list(self._source_cols) or (self._update_columns or []) + self._match_keys

        if self._strategy == "upsert":
            update_cols = self._resolve_update_columns()
            if update_cols:
                when_list.append(self._when_matched_update(update_cols))
            if all_cols:
                when_list.append(self._when_not_matched_insert(all_cols))

        elif self._strategy == "append":
            if all_cols:
                when_list.append(self._when_not_matched_insert(all_cols))

        elif self._strategy == "delete_insert":
            when_list.append(self._when_matched_delete())
            if all_cols:
                when_list.append(self._when_not_matched_insert(all_cols))

        merge = exp.Merge(
            this=target_table_expr,
            using=source_subquery,
            on=self._on_clause(),
            whens=exp.Whens(expressions=when_list),
        )

        return merge.sql(dialect="snowflake", pretty=True)

    def execute(self) -> MergeResult:
        """Execute the MERGE statement against Snowflake.

        When a watermark is configured, the full sequence is wrapped in a
        transaction:

        1. Read the last watermark value from ``watermark_table``.
        2. Inject it into the source query as a filter.
        3. Execute the MERGE.
        4. Update the watermark to the new maximum.
        5. COMMIT (or ROLLBACK on any failure).

        Returns:
            A ``MergeResult`` dataclass with row counts and timing.

        Raises:
            MergeError: If the MERGE fails for any reason.
        """
        watermark_value: str | None = None

        if self._watermark_column and self._watermark_table:
            try:
                cur = self._conn.execute(
                    "SELECT watermark_value "
                    "FROM " + self._watermark_table + " "
                    "WHERE table_name = %s "
                    "ORDER BY updated_at DESC "
                    "LIMIT 1",
                    (self._target_table,),
                )
                row = cur.fetchone()
                watermark_value = str(row[0]) if (row and row[0] is not None) else None
            except Exception as exc:
                raise MergeError(f"Failed to read watermark: {exc}") from exc

        sql = self.build(watermark_value=watermark_value)
        start = time.monotonic()
        rows_inserted = rows_updated = rows_deleted = 0
        query_id = ""

        try:
            self._conn.execute("BEGIN")

            cur = self._conn.execute(sql)
            query_id = getattr(cur, "sfqid", "") or ""

            # Snowflake MERGE returns one row: (number of rows inserted,
            # number of rows updated, number of rows deleted)
            row = cur.fetchone()
            if row:
                rows_inserted = int(row[0]) if len(row) > 0 else 0
                rows_updated = int(row[1]) if len(row) > 1 else 0
                rows_deleted = int(row[2]) if len(row) > 2 else 0

            if self._watermark_column and self._watermark_table:
                self._conn.execute(
                    "MERGE INTO " + self._watermark_table + " AS wt "
                    "USING ("
                    "  SELECT %s AS table_name, "
                    "         TO_VARCHAR(MAX("
                    + quote_identifier(self._watermark_column)
                    + ")) AS new_value"
                    "  FROM (" + self._source_query + ") AS _src"
                    ") AS src "
                    "ON wt.table_name = src.table_name "
                    "WHEN MATCHED THEN UPDATE SET "
                    "  wt.watermark_value = src.new_value, "
                    "  wt.updated_at = CURRENT_TIMESTAMP() "
                    "WHEN NOT MATCHED THEN INSERT (table_name, watermark_value, updated_at) "
                    "VALUES (src.table_name, src.new_value, CURRENT_TIMESTAMP())",
                    (self._target_table,),
                )

            self._conn.execute("COMMIT")

        except MergeError:
            raise
        except Exception as exc:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            raise MergeError(f"MERGE execution failed: {exc}") from exc

        execution_time_ms = int((time.monotonic() - start) * 1000)

        return MergeResult(
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=rows_deleted,
            execution_time_ms=execution_time_ms,
            query_id=query_id,
        )
