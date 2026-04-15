# snowcraft

> The Snowflake standard library that never shipped.

[![CI](https://github.com/AReyH/snowcraft/actions/workflows/ci.yml/badge.svg)](https://github.com/AReyH/snowcraft/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`snowcraft` is a Python toolkit that wraps the most common, tedious Snowflake
operations into clean, tested, pip-installable functions. Think of it as the
standard library that `snowflake-connector-python` never shipped.

---

## Install

```bash
pip install snowcraft
```

Python 3.10+ required.

---

## What's in the box

| Module | What it does |
| --------- | ------------------------------------------------------------ |
| `connection` | Context-managed Snowflake connection with env var fallback |
| `merge` | Programmatic `MERGE INTO` builder — inspect the SQL before running it |
| `schema` | Schema introspection and breaking-change detection |
| `profiler` | Find expensive queries and runaway warehouses |
| `scd` | SCD Type 1 and Type 2 dimension helpers |

---

## Quick start

### Inspect generated SQL without a Snowflake account

You can call `build()` to see exactly what SQL will be executed — no connection required:

```python
from unittest.mock import MagicMock
from snowcraft import MergeBuilder

builder = MergeBuilder(
    conn=MagicMock(),
    target_table="MYDB.PUBLIC.ORDERS",
    source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
    match_keys=["order_id"],
)
print(builder.build())
```

Output:

```sql
MERGE INTO "MYDB"."PUBLIC"."ORDERS" AS target
USING (
  SELECT
    order_id,
    status,
    updated_at
  FROM MYDB.STAGING.ORDERS
) AS source
ON target."order_id" = source."order_id"
WHEN MATCHED THEN UPDATE SET
  target."status" = source."status",
  target."updated_at" = source."updated_at"
WHEN NOT MATCHED THEN INSERT ("order_id", "status", "updated_at")
  VALUES (source."order_id", source."status", source."updated_at")
```

### Execute against Snowflake

```python
from snowcraft import SnowforgeConnection, MergeBuilder

with SnowforgeConnection() as conn:       # reads SNOWFLAKE_* env vars
    result = MergeBuilder(
        conn=conn,
        target_table="MYDB.PUBLIC.ORDERS",
        source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
        match_keys=["order_id"],
    ).execute()

    print(f"Inserted: {result.rows_inserted}, Updated: {result.rows_updated}")
```

### Merge strategies

**`append`** — insert new rows only, never touch existing ones:

```python
MergeBuilder(
    conn=conn,
    target_table="MYDB.PUBLIC.ORDERS",
    source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
    match_keys=["order_id"],
    strategy="append",
).execute()
```

**`delete_insert`** — delete matched rows, then re-insert all source rows (useful for full partition replacement):

```python
MergeBuilder(
    conn=conn,
    target_table="MYDB.PUBLIC.ORDERS",
    source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
    match_keys=["order_id"],
    strategy="delete_insert",
).execute()
```

### Watermark-based incremental loads

Track the last-loaded value automatically so each run only processes new rows:

```python
MergeBuilder(
    conn=conn,
    target_table="MYDB.PUBLIC.ORDERS",
    source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
    match_keys=["order_id"],
    watermark_column="updated_at",
    watermark_table="MYDB.PUBLIC.SNOWFORGE_WATERMARKS",
).execute()
# On the first run: loads everything.
# On subsequent runs: injects WHERE updated_at > <last_max> automatically.
```

The watermark table must have the schema `(table_name VARCHAR, watermark_value VARCHAR, updated_at TIMESTAMP)`. `snowcraft` creates or updates the row for you after each successful merge.

---

### Schema diffing

**`get_columns`** — fetch raw column metadata for a single table:

```python
from snowcraft import SchemaInspector

inspector = SchemaInspector(conn)
for col in inspector.get_columns("MYDB.PUBLIC.ORDERS"):
    print(col.name, col.data_type, "nullable" if col.is_nullable else "NOT NULL")
```

**`diff`** — compare two tables and get a structured diff:

```python
diff = inspector.diff("MYDB.STAGING.ORDERS", "MYDB.PUBLIC.ORDERS")
if diff.is_breaking:
    print(diff.to_markdown())    # ready to paste into a GitHub PR comment
```

Example output:

```markdown
## Schema Diff **[BREAKING]**

### Added columns

| Column | Type | Nullable | Default |
|--------|------|----------|---------|
| `discount` | `NUMBER(10,2)` | Yes | `—` |

### Removed columns ⚠️

| Column | Type | Nullable |
|--------|------|----------|
| `old_ref` | `VARCHAR(50)` | No |

### Type changes

| Column | Old type | New type | Breaking? |
|--------|----------|----------|-----------|
| `status` | `VARCHAR(256)` | `VARCHAR(64)` | Yes ⚠️ |
```

**`to_dict`** — serialize the diff to JSON for CI gating:

```python
import json

diff = inspector.diff("MYDB.STAGING.ORDERS", "MYDB.PUBLIC.ORDERS")
print(json.dumps(diff.to_dict(), indent=2))
# {"added": [...], "removed": [...], "type_changed": [...], "is_breaking": true}

# Exit non-zero in CI if the diff is breaking
if diff.is_breaking:
    raise SystemExit(1)
```

---

### Query profiling

**`top_expensive`** — find the costliest queries in the last N hours:

```python
from snowcraft import QueryProfiler

profiler = QueryProfiler(conn)
for q in profiler.top_expensive(n=10, lookback_hours=24):
    print(f"{q.query_id}  {q.execution_time_ms}ms  {q.credits_used:.4f} credits")
    for hint in q.optimization_hints:
        print(f"  hint: {hint}")
```

**`find_full_scans`** — queries scanning >80% of micro-partitions with >1 GB read:

```python
for q in profiler.find_full_scans(lookback_hours=24):
    ratio = q.partitions_scanned / q.partitions_total
    print(f"{q.query_id}  scanned {ratio:.0%} of partitions")
```

**`warehouse_cost`** — credit and USD breakdown, grouped by warehouse, user, or role:

```python
for row in profiler.warehouse_cost(lookback_days=7, group_by="warehouse"):
    print(f"{row.group_key}: {row.credits_used:.2f} credits  ${row.estimated_cost_usd:.2f}")

# Group by user to find who's spending the most
for row in profiler.warehouse_cost(lookback_days=30, group_by="user"):
    print(f"{row.group_key}: {row.query_count} queries  ${row.estimated_cost_usd:.2f}")
```

Credit price defaults to `$3.00`. Override it for Enterprise or Business Critical pricing:

```python
profiler.warehouse_cost(lookback_days=7, group_by="warehouse", credit_price_usd=4.00)
```

---

### SCD Type 1 and Type 2

**`apply_type1`** — overwrite existing records in place (no history kept):

```python
from snowcraft import SCDManager

SCDManager(
    conn=conn,
    target_table="MYDB.DW.DIM_CUSTOMER",
    source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
    business_keys=["customer_id"],
    tracked_columns=["name", "email"],
).apply_type1()
```

**`apply_type2`** — keep full version history by expiring old rows and inserting new ones:

```python
result = SCDManager(
    conn=conn,
    target_table="MYDB.DW.DIM_CUSTOMER",
    source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
    business_keys=["customer_id"],
    tracked_columns=["name", "email"],
).apply_type2()

print(f"Inserted: {result.rows_inserted}, Expired: {result.rows_expired}")
```

Type 2 requires your dimension table to have `effective_from`, `effective_to`, and `is_current` columns. Active rows store `'9999-12-31'` in `effective_to`. Column names are configurable:

```python
SCDManager(
    conn=conn,
    target_table="MYDB.DW.DIM_CUSTOMER",
    source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
    business_keys=["customer_id"],
    tracked_columns=["name", "email"],
    effective_from_col="valid_from",
    effective_to_col="valid_to",
    current_flag_col="is_active",
)
```

---

## Development

```bash
git clone https://github.com/AReyH/snowcraft.git
cd snowcraft
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

ruff check . && ruff format .
mypy snowcraft/
pytest tests/unit/ -v --cov=snowcraft --cov-report=term-missing
```

See [CLAUDE.md](CLAUDE.md) for the full project spec and contribution guide.

---

## License

MIT — see [LICENSE](LICENSE).
