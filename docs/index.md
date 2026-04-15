# snowcraft

> The Snowflake standard library that never shipped.

`snowcraft` is a Python toolkit that wraps the most common, tedious Snowflake
operations into clean, tested, pip-installable functions.

---

## Why snowcraft?

Data engineers who work with Snowflake spend a disproportionate amount of time
rewriting the same boilerplate: MERGE statements for incremental loads, schema
diffs before deployments, cost queries to find runaway warehouses, SCD Type 2
logic, and so on. `snowcraft` codifies those patterns into a stable, typed,
well-tested API so teams can stop copy-pasting and start importing.

## What snowcraft is NOT

- It is **not** an ORM — it does not abstract SQL away.
- It is **not** a replacement for dbt, SQLMesh, or any transformation framework.
- It does **not** manage Snowflake infrastructure (use Terraform for that).
- It does **not** handle authentication beyond what `snowflake-connector-python` already provides.

---

## Modules

| Module | Purpose |
|--------|---------|
| [`connection`](api/connection.md) | Context-managed connection wrapper |
| [`merge`](api/merge.md) | Incremental load / MERGE statement builder |
| [`schema`](api/schema.md) | Schema introspection and diffing |
| [`profiler`](api/profiler.md) | Query cost and performance analysis |
| [`scd`](api/scd.md) | Slowly Changing Dimension helpers (Type 1, 2) |

---

## Quick example

```python
from snowcraft import SnowforgeConnection, MergeBuilder

with SnowforgeConnection() as conn:
    result = MergeBuilder(
        conn=conn,
        target_table="MYDB.PUBLIC.ORDERS",
        source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
        match_keys=["order_id"],
    ).execute()

    print(f"Inserted: {result.rows_inserted}, Updated: {result.rows_updated}")
```

See the [Getting Started](getting-started.md) guide for a full walkthrough.
