"""Unit tests for snowcraft.profiler.

Tests cover heuristic hint generation logic, row-to-dataclass parsing,
query construction, and error handling — all without a real Snowflake
connection.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from snowcraft.exceptions import ProfilerError
from snowcraft.profiler import (
    CostSummary,
    QueryProfiler,
    QuerySummary,
    _generate_hints,
)

_DEFAULT_DT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _generate_hints
# ---------------------------------------------------------------------------


class TestGenerateHints:
    def _row(self, **overrides: object) -> dict[str, object]:
        defaults: dict[str, object] = {
            "bytes_scanned": 0,
            "partitions_scanned": 0,
            "partitions_total": 0,
            "rows_returned": 100,
            "rows_produced": 100,
            "query_type": "SELECT",
            "compilation_time_ms": 0,
            "execution_time_ms": 1000,
        }
        defaults.update(overrides)
        return defaults

    def test_no_hints_for_fast_small_query(self) -> None:
        hints = _generate_hints(self._row())
        assert hints == []

    def test_high_partition_ratio_hint(self) -> None:
        hints = _generate_hints(
            self._row(
                partitions_scanned=900,
                partitions_total=1000,
                bytes_scanned=2 * 1_073_741_824,
            )
        )
        assert any("partition" in h.lower() for h in hints)

    def test_zero_rows_returned_hint(self) -> None:
        hints = _generate_hints(
            self._row(
                bytes_scanned=2 * 1_073_741_824,
                rows_returned=0,
            )
        )
        assert any("0 rows" in h for h in hints)

    def test_high_compilation_time_hint(self) -> None:
        hints = _generate_hints(
            self._row(
                compilation_time_ms=8_000,
                execution_time_ms=10_000,
            )
        )
        assert any("ompilation" in h for h in hints)

    def test_large_dml_scan_hint(self) -> None:
        hints = _generate_hints(
            self._row(
                query_type="MERGE",
                bytes_scanned=12 * 1_073_741_824,
            )
        )
        assert any("MERGE" in h or "DML" in h for h in hints)

    def test_below_threshold_bytes_no_hint(self) -> None:
        hints = _generate_hints(
            self._row(
                partitions_scanned=900,
                partitions_total=1000,
                bytes_scanned=100,  # below 1 GB threshold
                rows_returned=0,
            )
        )
        # partition scan ratio hint requires bytes_scanned > 1 GB
        assert not any("partition" in h.lower() for h in hints)


# ---------------------------------------------------------------------------
# QueryProfiler.top_expensive()
# ---------------------------------------------------------------------------


def _make_query_row(
    query_id: str = "qid-001",
    query_text: str = "SELECT 1",
    user_name: str = "ARTURO",
    warehouse_name: str = "COMPUTE_WH",
    execution_time_ms: int = 5000,
    bytes_scanned: int = 0,
    partitions_scanned: int = 0,
    partitions_total: int = 0,
    rows_returned: int = 1,
    rows_produced: int = 1,
    query_type: str = "SELECT",
    compilation_time_ms: int = 50,
    credits_used: float = 0.01,
    start_time: datetime = _DEFAULT_DT,
) -> tuple[object, ...]:
    return (
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
    )


class TestQueryProfilerTopExpensive:
    def test_returns_list_of_query_summaries(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchall.return_value = [_make_query_row()]
        profiler = QueryProfiler(mock_conn)
        results = profiler.top_expensive(n=5)
        assert len(results) == 1
        assert isinstance(results[0], QuerySummary)

    def test_empty_result(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchall.return_value = []
        profiler = QueryProfiler(mock_conn)
        assert profiler.top_expensive() == []

    def test_query_summary_fields_populated(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchall.return_value = [
            _make_query_row(query_id="abc-999", user_name="BOB", execution_time_ms=10_000)
        ]
        profiler = QueryProfiler(mock_conn)
        q = profiler.top_expensive()[0]
        assert q.query_id == "abc-999"
        assert q.user_name == "BOB"
        assert q.execution_time_ms == 10_000

    def test_warehouse_filter_passed_to_execute(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchall.return_value = []
        profiler = QueryProfiler(mock_conn)
        profiler.top_expensive(warehouse="MY_WH")
        # The params tuple passed to execute should include the warehouse name
        _, kwargs = mock_conn.execute.call_args
        args = mock_conn.execute.call_args[0]
        params = args[1] if len(args) > 1 else kwargs.get("params")
        assert params is not None
        assert "MY_WH" in params

    def test_execute_failure_raises_profiler_error(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = RuntimeError("access denied")
        profiler = QueryProfiler(mock_conn)
        with pytest.raises(ProfilerError, match="QUERY_HISTORY"):
            profiler.top_expensive()


# ---------------------------------------------------------------------------
# QueryProfiler.find_full_scans()
# ---------------------------------------------------------------------------


class TestQueryProfilerFindFullScans:
    def test_returns_list(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchall.return_value = [
            _make_query_row(
                bytes_scanned=2 * 1_073_741_824,
                partitions_scanned=900,
                partitions_total=1000,
            )
        ]
        profiler = QueryProfiler(mock_conn)
        results = profiler.find_full_scans()
        assert len(results) == 1

    def test_full_scan_result_has_hints(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchall.return_value = [
            _make_query_row(
                bytes_scanned=2 * 1_073_741_824,
                partitions_scanned=900,
                partitions_total=1000,
                rows_returned=0,
            )
        ]
        profiler = QueryProfiler(mock_conn)
        results = profiler.find_full_scans()
        assert results[0].optimization_hints


# ---------------------------------------------------------------------------
# QueryProfiler.warehouse_cost()
# ---------------------------------------------------------------------------


class TestQueryProfilerWarehouseCost:
    def test_warehouse_group_by(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchall.return_value = [("COMPUTE_WH", 10.5)]
        profiler = QueryProfiler(mock_conn)
        results = profiler.warehouse_cost(group_by="warehouse")
        assert len(results) == 1
        assert isinstance(results[0], CostSummary)
        assert results[0].group_key == "COMPUTE_WH"
        assert results[0].credits_used == 10.5

    def test_estimated_cost_usd_calculated(
        self, mock_conn: MagicMock, mock_cursor: MagicMock
    ) -> None:
        mock_cursor.fetchall.return_value = [("COMPUTE_WH", 4.0)]
        profiler = QueryProfiler(mock_conn)
        results = profiler.warehouse_cost(credit_price_usd=3.0)
        assert results[0].estimated_cost_usd == pytest.approx(12.0)

    def test_user_group_by(self, mock_conn: MagicMock, mock_cursor: MagicMock) -> None:
        mock_cursor.fetchall.return_value = [("BOB", 2.0, 150)]
        profiler = QueryProfiler(mock_conn)
        results = profiler.warehouse_cost(group_by="user")
        assert results[0].query_count == 150

    def test_invalid_group_by_raises(self, mock_conn: MagicMock) -> None:
        profiler = QueryProfiler(mock_conn)
        with pytest.raises(ProfilerError, match="Invalid group_by"):
            profiler.warehouse_cost(group_by="department")  # type: ignore[arg-type]

    def test_query_failure_raises_profiler_error(self, mock_conn: MagicMock) -> None:
        mock_conn.execute.side_effect = RuntimeError("access denied")
        profiler = QueryProfiler(mock_conn)
        with pytest.raises(ProfilerError, match="Failed to query cost data"):
            profiler.warehouse_cost()
