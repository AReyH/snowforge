"""Query cost and performance analysis.

Wraps Snowflake's ``QUERY_HISTORY`` and ``WAREHOUSE_METERING_HISTORY`` views to
surface expensive queries, full-table scans, and cost attribution by warehouse,
user, or role.

All optimization hints in this module are generated heuristically from query
statistics — no external AI or LLM calls are made.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

from snowcraft.connection import SnowforgeConnection
from snowcraft.exceptions import ProfilerError

# Default USD cost per credit. Enterprise vs. Business Critical pricing differs;
# override via the ``credit_price_usd`` parameter on individual methods.
_DEFAULT_CREDIT_PRICE_USD = 3.0

# Thresholds for heuristic hint generation
_FULL_SCAN_PARTITION_RATIO = 0.8
_FULL_SCAN_MIN_BYTES = 1_073_741_824  # 1 GB


@dataclass
class QuerySummary:
    """Summary of a single Snowflake query with performance metadata.

    Attributes:
        query_id: Snowflake-assigned query ID.
        query_text: The SQL text (truncated by Snowflake to 100 KB).
        user_name: Snowflake user who executed the query.
        warehouse_name: Virtual warehouse that ran the query.
        execution_time_ms: Total query execution time in milliseconds.
        bytes_scanned: Total bytes scanned from micro-partitions.
        partitions_scanned: Number of micro-partitions accessed.
        partitions_total: Total micro-partitions in the scanned tables.
        credits_used: Credits consumed by the query (approximate).
        start_time: UTC timestamp when the query started.
        optimization_hints: Heuristic suggestions based on query statistics.
    """

    query_id: str
    query_text: str
    user_name: str
    warehouse_name: str
    execution_time_ms: int
    bytes_scanned: int
    partitions_scanned: int
    partitions_total: int
    credits_used: float
    start_time: datetime
    optimization_hints: list[str] = field(default_factory=list)


@dataclass
class CostSummary:
    """Aggregated cost attribution for a warehouse, user, or role.

    Attributes:
        group_key: The group value (e.g. warehouse name, user name, role name).
        credits_used: Total credits consumed.
        estimated_cost_usd: Estimated USD cost based on the configured credit price.
        query_count: Number of queries contributing to this group.
    """

    group_key: str
    credits_used: float
    estimated_cost_usd: float
    query_count: int


# ---------------------------------------------------------------------------
# Heuristic hint generation
# ---------------------------------------------------------------------------


def _generate_hints(row: dict[str, Any]) -> list[str]:
    """Produce a list of optimization hints from a QUERY_HISTORY row dict.

    Args:
        row: A dict with keys matching the column aliases returned by
            ``_EXPENSIVE_QUERY`` / ``_FULL_SCAN_QUERY``.

    Returns:
        A possibly-empty list of human-readable hint strings.
    """
    hints: list[str] = []

    bytes_scanned = int(row.get("bytes_scanned") or 0)
    partitions_scanned = int(row.get("partitions_scanned") or 0)
    partitions_total = int(row.get("partitions_total") or 0)
    rows_returned = int(row.get("rows_returned") or 0)
    rows_produced = int(row.get("rows_produced") or 0)
    execution_time_ms = int(row.get("execution_time_ms") or 0)
    query_type = str(row.get("query_type") or "").upper()
    compilation_time_ms = int(row.get("compilation_time_ms") or 0)

    # High partition scan ratio
    if partitions_total > 0:
        ratio = partitions_scanned / partitions_total
        if ratio > _FULL_SCAN_PARTITION_RATIO and bytes_scanned > _FULL_SCAN_MIN_BYTES:
            hints.append(
                f"High partition scan ratio ({ratio:.0%}) — consider adding or "
                "adjusting a cluster key on the scanned table(s) to improve pruning."
            )

    # Large scan with zero or minimal rows returned
    if bytes_scanned > _FULL_SCAN_MIN_BYTES and rows_returned == 0:
        hints.append(
            f"Query scanned {bytes_scanned / 1e9:.1f} GB but returned 0 rows — "
            "verify that filter conditions are using partition-prunable columns."
        )
    elif bytes_scanned > _FULL_SCAN_MIN_BYTES and rows_produced > 0 and rows_returned == 0:
        hints.append(
            "Large intermediate result set was produced but no rows were returned — "
            "check for a restrictive HAVING or outer query filter that could be pushed down."
        )

    # Very long compilation relative to execution
    if compilation_time_ms > 0 and execution_time_ms > 0:
        if compilation_time_ms > execution_time_ms * 0.5 and compilation_time_ms > 5_000:
            hints.append(
                f"Compilation time ({compilation_time_ms:,} ms) is more than 50% of "
                "execution time — consider simplifying complex CTEs or very large IN lists."
            )

    # DML on large scans
    _dml_types = ("INSERT", "UPDATE", "DELETE", "MERGE")
    if query_type in _dml_types and bytes_scanned > 10 * _FULL_SCAN_MIN_BYTES:
        hints.append(
            f"DML query ({query_type}) scanned {bytes_scanned / 1e9:.1f} GB — "
            "ensure WHERE clause columns are cluster keys or consider "
            "using micro-partition pruning."
        )

    return hints


# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

_EXPENSIVE_QUERY_BASE = """
SELECT
    QUERY_ID                                    AS query_id,
    QUERY_TEXT                                  AS query_text,
    USER_NAME                                   AS user_name,
    WAREHOUSE_NAME                              AS warehouse_name,
    TOTAL_ELAPSED_TIME                          AS execution_time_ms,
    BYTES_SCANNED                               AS bytes_scanned,
    PARTITIONS_SCANNED                          AS partitions_scanned,
    PARTITIONS_TOTAL                            AS partitions_total,
    ROWS_RETURNED                               AS rows_returned,
    ROWS_PRODUCED                               AS rows_produced,
    QUERY_TYPE                                  AS query_type,
    COMPILATION_TIME                            AS compilation_time_ms,
    COALESCE(CREDITS_USED_CLOUD_SERVICES, 0)   AS credits_used,
    START_TIME                                  AS start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE EXECUTION_STATUS = 'SUCCESS'
  AND START_TIME >= DATEADD('hour', -%s, CURRENT_TIMESTAMP())
"""

_EXPENSIVE_QUERY_WAREHOUSE_FILTER = "  AND WAREHOUSE_NAME = %s\n"
_EXPENSIVE_QUERY_FULL_SCAN_FILTER = (
    "  AND PARTITIONS_TOTAL > 0\n"
    "  AND (PARTITIONS_SCANNED::FLOAT / PARTITIONS_TOTAL) > %s\n"
    "  AND BYTES_SCANNED > %s\n"
)
_EXPENSIVE_QUERY_ORDER = "ORDER BY TOTAL_ELAPSED_TIME DESC\nLIMIT %s"

_WAREHOUSE_COST_QUERY_WAREHOUSE = """
SELECT
    WAREHOUSE_NAME  AS group_key,
    SUM(CREDITS_USED) AS credits_used,
    COUNT(*)        AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE EXECUTION_STATUS = 'SUCCESS'
  AND START_TIME >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC
"""

_WAREHOUSE_COST_QUERY_USER = """
SELECT
    USER_NAME       AS group_key,
    SUM(COALESCE(CREDITS_USED_CLOUD_SERVICES, 0)) AS credits_used,
    COUNT(*)        AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE EXECUTION_STATUS = 'SUCCESS'
  AND START_TIME >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC
"""

_WAREHOUSE_COST_QUERY_ROLE = """
SELECT
    ROLE_NAME       AS group_key,
    SUM(COALESCE(CREDITS_USED_CLOUD_SERVICES, 0)) AS credits_used,
    COUNT(*)        AS query_count
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE EXECUTION_STATUS = 'SUCCESS'
  AND START_TIME >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC
"""

_WAREHOUSE_METERING_QUERY = """
SELECT
    WAREHOUSE_NAME  AS group_key,
    SUM(CREDITS_USED) AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -%s, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC
"""


# ---------------------------------------------------------------------------
# Internal row parsers
# ---------------------------------------------------------------------------


def _row_to_query_summary(row: tuple[Any, ...]) -> QuerySummary:
    """Convert a raw QUERY_HISTORY result row into a ``QuerySummary``."""
    (
        query_id,
        query_text,
        user_name,
        warehouse_name,
        execution_time_ms,
        bytes_scanned,
        partitions_scanned,
        partitions_total,
        rows_returned,
        rows_produced,
        query_type,
        compilation_time_ms,
        credits_used,
        start_time,
    ) = row

    row_dict: dict[str, object] = {
        "bytes_scanned": bytes_scanned,
        "partitions_scanned": partitions_scanned,
        "partitions_total": partitions_total,
        "rows_returned": rows_returned,
        "rows_produced": rows_produced,
        "query_type": query_type,
        "compilation_time_ms": compilation_time_ms,
        "execution_time_ms": execution_time_ms,
    }
    hints = _generate_hints(row_dict)

    return QuerySummary(
        query_id=str(query_id),
        query_text=str(query_text),
        user_name=str(user_name or ""),
        warehouse_name=str(warehouse_name or ""),
        execution_time_ms=int(execution_time_ms or 0),
        bytes_scanned=int(bytes_scanned or 0),
        partitions_scanned=int(partitions_scanned or 0),
        partitions_total=int(partitions_total or 0),
        credits_used=float(credits_used or 0.0),
        start_time=(
            start_time
            if isinstance(start_time, datetime)
            else datetime.fromisoformat(str(start_time))
        ),
        optimization_hints=hints,
    )


# ---------------------------------------------------------------------------
# QueryProfiler
# ---------------------------------------------------------------------------


class QueryProfiler:
    """Surfaces expensive queries, full-table scans, and cost attribution.

    Uses ``SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`` (not
    ``INFORMATION_SCHEMA.QUERY_HISTORY``) so lookback windows longer than 7
    days are supported. Requires the executing role to have access to the
    ``SNOWFLAKE`` database.

    Args:
        conn: An open ``SnowforgeConnection``.

    Example:
        profiler = QueryProfiler(conn)
        expensive = profiler.top_expensive(n=10, lookback_hours=48)
        for q in expensive:
            print(q.query_id, q.execution_time_ms, q.optimization_hints)
    """

    def __init__(self, conn: SnowforgeConnection) -> None:
        self._conn = conn

    def top_expensive(
        self,
        n: int = 20,
        lookback_hours: int = 24,
        warehouse: str | None = None,
    ) -> list[QuerySummary]:
        """Return the *n* most expensive queries by total elapsed time.

        Args:
            n: Maximum number of results to return.
            lookback_hours: How many hours back to search from now.
            warehouse: Optional warehouse name to filter to. ``None`` returns
                queries from all warehouses.

        Returns:
            Ordered list (most expensive first) of ``QuerySummary`` objects.

        Raises:
            ProfilerError: If the query history view is inaccessible or the
                query fails.
        """
        sql = _EXPENSIVE_QUERY_BASE
        params: list[object] = [lookback_hours]

        if warehouse:
            sql += _EXPENSIVE_QUERY_WAREHOUSE_FILTER
            params.append(warehouse)

        sql += _EXPENSIVE_QUERY_ORDER
        params.append(n)

        try:
            cur = self._conn.execute(sql, tuple(params))
            rows = cur.fetchall()
        except Exception as exc:
            raise ProfilerError(
                f"Failed to query QUERY_HISTORY (check that your role has access "
                f"to SNOWFLAKE.ACCOUNT_USAGE): {exc}"
            ) from exc

        return [_row_to_query_summary(row) for row in rows]

    def find_full_scans(self, lookback_hours: int = 24) -> list[QuerySummary]:
        """Return queries that performed near-full micro-partition scans.

        A query is classified as a full scan when:
        ``partitions_scanned / partitions_total > 0.8`` **and**
        ``bytes_scanned > 1 GB``.

        Args:
            lookback_hours: How many hours back to search from now.

        Returns:
            List of ``QuerySummary`` objects that triggered the full-scan rule,
            ordered by total elapsed time (longest first).

        Raises:
            ProfilerError: If the query history view is inaccessible.
        """
        sql = _EXPENSIVE_QUERY_BASE + _EXPENSIVE_QUERY_FULL_SCAN_FILTER + _EXPENSIVE_QUERY_ORDER
        params = (
            lookback_hours,
            _FULL_SCAN_PARTITION_RATIO,
            _FULL_SCAN_MIN_BYTES,
            1000,  # generous limit; callers can slice the result
        )

        try:
            cur = self._conn.execute(sql, params)
            rows = cur.fetchall()
        except Exception as exc:
            raise ProfilerError(f"Failed to query QUERY_HISTORY for full scans: {exc}") from exc

        return [_row_to_query_summary(row) for row in rows]

    def warehouse_cost(
        self,
        lookback_days: int = 7,
        group_by: Literal["warehouse", "user", "role"] = "warehouse",
        credit_price_usd: float = _DEFAULT_CREDIT_PRICE_USD,
    ) -> list[CostSummary]:
        """Return aggregated credit usage and estimated USD cost.

        For ``group_by="warehouse"`` this queries ``WAREHOUSE_METERING_HISTORY``
        for precise credit accounting. For ``"user"`` and ``"role"`` it uses
        ``QUERY_HISTORY`` cloud-services credits (which undercount compute credits
        but is the best available attribution).

        Args:
            lookback_days: How many calendar days back to aggregate.
            group_by: Dimension to group by. One of ``"warehouse"``,
                ``"user"``, or ``"role"``.
            credit_price_usd: USD price per credit. Defaults to
                ``$3.00`` (Standard Edition). Override for Enterprise /
                Business Critical pricing.

        Returns:
            List of ``CostSummary`` objects ordered by ``credits_used`` desc.

        Raises:
            ProfilerError: If the metering or query history view is inaccessible.
        """
        if group_by == "warehouse":
            sql = _WAREHOUSE_METERING_QUERY
        elif group_by == "user":
            sql = _WAREHOUSE_COST_QUERY_USER
        elif group_by == "role":
            sql = _WAREHOUSE_COST_QUERY_ROLE
        else:
            raise ProfilerError(
                f"Invalid group_by value '{group_by}'. Must be one of: 'warehouse', 'user', 'role'."
            )

        try:
            cur = self._conn.execute(sql, (lookback_days,))
            rows = cur.fetchall()
        except Exception as exc:
            raise ProfilerError(
                f"Failed to query cost data (group_by={group_by!r}): {exc}"
            ) from exc

        results: list[CostSummary] = []
        for row in rows:
            group_key = str(row[0] or "UNKNOWN")
            credits = float(row[1] or 0.0)
            # WAREHOUSE_METERING_HISTORY has only 2 columns; QUERY_HISTORY has 3
            query_count = int(row[2]) if len(row) > 2 else 0
            results.append(
                CostSummary(
                    group_key=group_key,
                    credits_used=credits,
                    estimated_cost_usd=round(credits * credit_price_usd, 4),
                    query_count=query_count,
                )
            )

        return results
