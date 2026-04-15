"""Slowly Changing Dimension (SCD) helpers for Type 1 and Type 2 patterns.

Type 1 overwrites the current record; Type 2 maintains full version history by
expiring old versions and inserting new ones. Both operations are executed
within a Snowflake transaction using multi-statement support.
"""

from __future__ import annotations

from dataclasses import dataclass

from snowcraft.connection import SnowforgeConnection
from snowcraft.exceptions import MergeError
from snowcraft.merge import MergeBuilder, MergeResult
from snowcraft.utils import quote_identifier, quote_table

# Industry-standard sentinel for "currently active" in SCD Type 2 tables.
# Using a concrete date (rather than NULL) simplifies range queries with
# BETWEEN and avoids three-valued logic surprises.
_SCD2_OPEN_DATE = "9999-12-31"


@dataclass
class SCDResult:
    """Statistics returned after a Type 2 SCD operation.

    Attributes:
        rows_inserted: Newly inserted version records.
        rows_expired: Previous versions that were closed out
            (``effective_to`` set and ``is_current`` set to FALSE).
        rows_unchanged: Source rows that matched existing current records
            with identical tracked-column values — no action taken.
        query_id: Snowflake query ID for the final INSERT statement.
    """

    rows_inserted: int
    rows_expired: int
    rows_unchanged: int
    query_id: str


class SCDManager:
    """Applies SCD Type 1 or Type 2 logic to a Snowflake dimension table.

    Type 1 is delegated to ``MergeBuilder`` (it is a plain upsert).

    Type 2 uses two SQL statements wrapped in a transaction:

    1. ``UPDATE`` existing ``is_current = TRUE`` records where any tracked
       column has changed — setting ``effective_to = CURRENT_TIMESTAMP()`` and
       ``is_current = FALSE``.
    2. ``INSERT`` new version rows (with ``effective_from = CURRENT_TIMESTAMP()``,
       ``effective_to = '9999-12-31'``, and ``is_current = TRUE``) for every
       source row that either has no match in the target or where tracked
       columns differ.

    Args:
        conn: An open ``SnowforgeConnection``.
        target_table: Fully-qualified SCD dimension table, e.g.
            ``"DB.DW.DIM_CUSTOMER"``.
        source_query: A ``SELECT`` statement whose output is the incoming data.
        business_keys: Columns that uniquely identify a real-world entity
            across all versions (the ``ON`` clause key).
        tracked_columns: Columns whose value changes should trigger a new SCD2
            version row.
        effective_from_col: Name of the column storing the version start date/ts.
        effective_to_col: Name of the column storing the version end date/ts.
            Active rows store ``'9999-12-31'``.
        current_flag_col: Name of the boolean column that marks the active version.

    Example:
        manager = SCDManager(
            conn=conn,
            target_table="MYDB.DW.DIM_CUSTOMER",
            source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
            business_keys=["customer_id"],
            tracked_columns=["name", "email"],
        )
        result = manager.apply_type2()
        print(result.rows_inserted, result.rows_expired)
    """

    def __init__(
        self,
        conn: SnowforgeConnection,
        target_table: str,
        source_query: str,
        business_keys: list[str],
        tracked_columns: list[str],
        effective_from_col: str = "effective_from",
        effective_to_col: str = "effective_to",
        current_flag_col: str = "is_current",
    ) -> None:
        if not business_keys:
            raise MergeError("business_keys cannot be empty.")
        if not tracked_columns:
            raise MergeError("tracked_columns cannot be empty.")

        self._conn = conn
        self._target_table = target_table
        self._source_query = source_query
        self._business_keys = business_keys
        self._tracked_columns = tracked_columns
        self._effective_from_col = effective_from_col
        self._effective_to_col = effective_to_col
        self._current_flag_col = current_flag_col

    # ------------------------------------------------------------------
    # Internal SQL builders
    # ------------------------------------------------------------------

    def _join_condition(self, target_alias: str, source_alias: str) -> str:
        """Build an SQL join predicate for the business key columns."""
        parts = [
            f"{target_alias}.{quote_identifier(k)} = {source_alias}.{quote_identifier(k)}"
            for k in self._business_keys
        ]
        return " AND ".join(parts)

    def _change_condition(self, target_alias: str, source_alias: str) -> str:
        """Build the OR-chained predicate that detects tracked-column changes."""
        parts = [
            f"NOT ({target_alias}.{quote_identifier(c)} <=> {source_alias}.{quote_identifier(c)})"
            for c in self._tracked_columns
        ]
        return "(\n      " + "\n      OR ".join(parts) + "\n    )"

    def _build_expire_sql(self) -> str:
        """Return the UPDATE statement that expires changed current records."""
        tgt = quote_table(self._target_table)
        eff_to = quote_identifier(self._effective_to_col)
        is_current = quote_identifier(self._current_flag_col)
        join_cond = self._join_condition("t", "src")
        change_cond = self._change_condition("t", "src")

        return (
            f"UPDATE {tgt} AS t\n"
            f"SET\n"
            f"    t.{eff_to} = CURRENT_TIMESTAMP(),\n"
            f"    t.{is_current} = FALSE\n"
            f"FROM (\n"
            f"    {self._source_query}\n"
            f") AS src\n"
            f"WHERE {join_cond}\n"
            f"  AND t.{is_current} = TRUE\n"
            f"  AND {change_cond}"
        )

    def _build_insert_sql(self, source_columns: list[str]) -> str:
        """Return the INSERT statement that adds new version rows.

        Inserts rows from the source that either:
        * Have no matching business key in the target (new entities), or
        * Have a match but the existing current record was just expired
          (tracked column changed).
        """
        tgt = quote_table(self._target_table)
        eff_from = quote_identifier(self._effective_from_col)
        eff_to = quote_identifier(self._effective_to_col)
        is_current = quote_identifier(self._current_flag_col)
        join_cond = self._join_condition("t", "src")
        change_cond = self._change_condition("t", "src")

        src_cols_quoted = ", ".join(f"src.{quote_identifier(c)}" for c in source_columns)
        insert_cols_quoted = ", ".join(quote_identifier(c) for c in source_columns)

        return (
            f"INSERT INTO {tgt} (\n"
            f"    {insert_cols_quoted},\n"
            f"    {eff_from},\n"
            f"    {eff_to},\n"
            f"    {is_current}\n"
            f")\n"
            f"SELECT\n"
            f"    {src_cols_quoted},\n"
            f"    CURRENT_TIMESTAMP(),\n"
            f"    '{_SCD2_OPEN_DATE}',\n"
            f"    TRUE\n"
            f"FROM (\n"
            f"    {self._source_query}\n"
            f") AS src\n"
            f"LEFT JOIN {tgt} AS t\n"
            f"    ON {join_cond} AND t.{is_current} = TRUE\n"
            f"WHERE t.{quote_identifier(self._business_keys[0])} IS NULL\n"
            f"   OR {change_cond}"
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def apply_type1(self) -> MergeResult:
        """Apply SCD Type 1 logic (overwrite matching records).

        Delegates directly to ``MergeBuilder`` using the ``upsert`` strategy.
        All ``tracked_columns`` plus any non-business-key columns from the
        source are updated on match.

        Returns:
            A ``MergeResult`` with row counts.

        Raises:
            MergeError: If the underlying MERGE fails.
        """
        builder = MergeBuilder(
            conn=self._conn,
            target_table=self._target_table,
            source_query=self._source_query,
            match_keys=self._business_keys,
            strategy="upsert",
            update_columns=self._tracked_columns,
        )
        return builder.execute()

    def apply_type2(self) -> SCDResult:
        """Apply SCD Type 2 logic (versioned history with expiry).

        Executes two statements in a single transaction:

        1. Expire changed current records (``UPDATE … SET effective_to``,
           ``is_current = FALSE``).
        2. Insert new version rows for new and changed records.

        The ``effective_to`` for active records is set to ``'9999-12-31'``
        following the industry convention for open-ended ranges.

        Returns:
            An ``SCDResult`` with counts of inserted and expired rows.

        Raises:
            MergeError: If either statement fails or the transaction cannot
                be committed.
        """
        # Determine source columns by inspecting the SELECT column list.
        # We need explicit column names to build the INSERT statement.
        from snowcraft.merge import _extract_select_columns  # avoid circular at module level

        source_columns = _extract_select_columns(self._source_query)
        if not source_columns:
            raise MergeError(
                "apply_type2() requires an explicit column list in source_query "
                "(not SELECT *) so the INSERT column list can be determined statically."
            )

        expire_sql = self._build_expire_sql()
        insert_sql = self._build_insert_sql(source_columns)

        rows_expired = 0
        rows_inserted = 0
        query_id = ""

        try:
            self._conn.execute("BEGIN")

            # Step 1: expire changed records
            cur = self._conn.execute(expire_sql)
            row = cur.fetchone()
            rows_expired = int(row[0]) if row else 0

            # Step 2: insert new versions
            cur = self._conn.execute(insert_sql)
            query_id = getattr(cur, "sfqid", "") or ""
            row = cur.fetchone()
            rows_inserted = int(row[0]) if row else 0

            self._conn.execute("COMMIT")

        except MergeError:
            raise
        except Exception as exc:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            raise MergeError(f"SCD Type 2 operation failed: {exc}") from exc

        # rows_unchanged = total source rows − rows that were inserted
        # We don't have a cheap way to count this without an extra query, so
        # we approximate as 0. Callers that need this stat should query the
        # target table directly.
        return SCDResult(
            rows_inserted=rows_inserted,
            rows_expired=rows_expired,
            rows_unchanged=0,
            query_id=query_id,
        )
