# Getting Started

## Installation

```bash
pip install snowcraft
```

Python 3.10+ required.

---

## Configuration

snowcraft reads Snowflake credentials from environment variables by default:

```bash
export SNOWFLAKE_ACCOUNT=your_account.us-east-1
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=MYDB
export SNOWFLAKE_SCHEMA=PUBLIC
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_ROLE=SYSADMIN
```

You can also pass credentials directly:

```python
from snowcraft import SnowforgeConnection

conn = SnowforgeConnection(
    account="your_account.us-east-1",
    user="your_user",
    password="your_password",
    database="MYDB",
)
```

---

## Incremental Loads with MERGE

`MergeBuilder` generates and executes Snowflake `MERGE INTO` statements. The
generated SQL is always inspectable before execution via `build()`.

```python
from snowcraft import SnowforgeConnection, MergeBuilder

with SnowforgeConnection() as conn:
    builder = MergeBuilder(
        conn=conn,
        target_table="MYDB.PUBLIC.ORDERS",
        source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
        match_keys=["order_id"],
    )

    # Inspect the SQL before running it
    print(builder.build())

    # Execute and get stats
    result = builder.execute()
    print(f"Inserted: {result.rows_inserted}, Updated: {result.rows_updated}")
```

### Watermark-based incremental loads

```python
builder = MergeBuilder(
    conn=conn,
    target_table="MYDB.PUBLIC.EVENTS",
    source_query="SELECT event_id, event_type, created_at FROM MYDB.STAGING.EVENTS",
    match_keys=["event_id"],
    watermark_column="created_at",
    watermark_table="MYDB.PUBLIC.SNOWFORGE_WATERMARKS",
)
result = builder.execute()  # automatically filters and updates the watermark
```

---

## Schema Diffing

Use `SchemaInspector` to detect breaking schema changes before a deployment or
migration. The output is clean enough to post directly in a GitHub PR comment.

```python
from snowcraft import SnowforgeConnection, SchemaInspector

with SnowforgeConnection() as conn:
    inspector = SchemaInspector(conn)

    diff = inspector.diff(
        source="MYDB.STAGING.ORDERS",   # proposed new schema
        target="MYDB.PUBLIC.ORDERS",    # current production schema
    )

    if diff.is_breaking:
        print("BREAKING CHANGES DETECTED")
        print(diff.to_markdown())  # paste into a GitHub PR comment

    # Machine-readable for CI gating
    import json
    print(json.dumps(diff.to_dict(), indent=2))
```

---

## Query Profiling

`QueryProfiler` surfaces expensive queries and cost attribution without
leaving the Python REPL.

```python
from snowcraft import SnowforgeConnection, QueryProfiler

with SnowforgeConnection() as conn:
    profiler = QueryProfiler(conn)

    # Top 20 slowest queries in the last 24 hours
    for q in profiler.top_expensive(n=20):
        print(q.query_id, q.execution_time_ms, "ms")
        for hint in q.optimization_hints:
            print(f"  HINT: {hint}")

    # Queries that scanned > 80% of their partitions and > 1 GB
    full_scans = profiler.find_full_scans(lookback_hours=48)

    # Cost by warehouse over the last 7 days
    costs = profiler.warehouse_cost(lookback_days=7, group_by="warehouse")
    for c in costs:
        print(f"{c.group_key}: ${c.estimated_cost_usd:.2f} ({c.credits_used:.2f} credits)")
```

---

## Slowly Changing Dimensions

`SCDManager` handles both Type 1 (overwrite) and Type 2 (versioned history).

### SCD Type 1 (overwrite)

```python
from snowcraft import SnowforgeConnection, SCDManager

with SnowforgeConnection() as conn:
    manager = SCDManager(
        conn=conn,
        target_table="MYDB.DW.DIM_CUSTOMER",
        source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
        business_keys=["customer_id"],
        tracked_columns=["name", "email"],
    )
    result = manager.apply_type1()
```

### SCD Type 2 (versioned history)

```python
result = manager.apply_type2()
print(f"New versions: {result.rows_inserted}, Expired: {result.rows_expired}")
```

Your target table must have `effective_from`, `effective_to`, and `is_current`
columns (customisable via the constructor arguments). Active records will have
`effective_to = '9999-12-31'` following the industry convention.

---

## Error handling

All snowcraft errors inherit from `SnowforgeError`:

```python
from snowcraft.exceptions import SnowforgeError, MergeError

try:
    result = builder.execute()
except MergeError as e:
    print(f"Merge failed: {e}")
    # e.__cause__ contains the original Snowflake connector exception
except SnowforgeError as e:
    print(f"Snowforge error: {e}")
```
