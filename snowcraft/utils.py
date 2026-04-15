"""Internal shared utilities for snowcraft.

This module is not part of the public API. Functions here may change without
notice between releases. Import from ``snowcraft`` directly for the stable API.
"""

from __future__ import annotations

import sqlglot.expressions as exp


def quote_identifier(name: str) -> str:
    """Return a safely double-quoted Snowflake identifier.

    Uses sqlglot to produce a consistently escaped identifier string. The name
    is never interpolated into SQL directly — always pass through this function
    first.

    Args:
        name: The raw identifier name (column, table, schema, database).

    Returns:
        The identifier wrapped in double quotes with any internal double-quotes
        escaped, suitable for embedding in Snowflake SQL.

    Example:
        >>> quote_identifier("my_table")
        '"my_table"'
        >>> quote_identifier('weird"name')
        '"weird""name"'
    """
    return exp.Identifier(this=name, quoted=True).sql(dialect="snowflake")


def quote_table(table: str) -> str:
    """Return a safely double-quoted, fully-qualified Snowflake table reference.

    Accepts one-, two-, or three-part identifiers separated by dots. Each part
    is individually quoted.

    Args:
        table: A dot-separated table reference, e.g. ``"DB.SCHEMA.TABLE"``,
            ``"SCHEMA.TABLE"``, or ``"TABLE"``.

    Returns:
        A fully-quoted Snowflake table reference, e.g.
        ``'"DB"."SCHEMA"."TABLE"'``.

    Example:
        >>> quote_table("mydb.public.orders")
        '"mydb"."public"."orders"'
    """
    parts = table.split(".")
    return ".".join(quote_identifier(p) for p in parts)


def parse_table_parts(table: str) -> tuple[str | None, str | None, str]:
    """Split a dot-separated table reference into (catalog, db, table) parts.

    Args:
        table: A one-, two-, or three-part dot-separated table name.

    Returns:
        A tuple of (catalog, schema, table_name). Missing parts are ``None``.
    """
    parts = table.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return None, parts[0], parts[1]
    else:
        return None, None, parts[0]


def build_table_expr(table: str) -> exp.Table:
    """Build a sqlglot ``Table`` expression from a dot-separated table name.

    Args:
        table: A fully-qualified Snowflake table name.

    Returns:
        A sqlglot ``Table`` expression with each part individually quoted.
    """
    catalog, db, tbl = parse_table_parts(table)
    return exp.Table(
        this=exp.Identifier(this=tbl, quoted=True),
        db=exp.Identifier(this=db, quoted=True) if db else None,
        catalog=exp.Identifier(this=catalog, quoted=True) if catalog else None,
    )
