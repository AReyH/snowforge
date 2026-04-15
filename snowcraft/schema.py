"""Schema introspection and diffing for Snowflake tables.

Provides structured access to Snowflake column metadata via
``INFORMATION_SCHEMA.COLUMNS`` and a diff engine that compares two tables and
classifies changes as breaking or non-breaking.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from snowcraft.connection import SnowforgeConnection
from snowcraft.exceptions import SchemaError


@dataclass
class ColumnDef:
    """Metadata for a single column in a Snowflake table.

    Attributes:
        name: Column name exactly as stored in Snowflake.
        data_type: Snowflake data type string (e.g. ``"TEXT"``, ``"NUMBER(38,0)"``).
        is_nullable: Whether the column allows ``NULL`` values.
        default: Column default expression, or ``None`` if no default is set.
        comment: Column-level comment, or ``None`` if none.
    """

    name: str
    data_type: str
    is_nullable: bool
    default: str | None
    comment: str | None


@dataclass
class SchemaDiff:
    """Structured diff between a source and target table's column definitions.

    Attributes:
        added: Columns present in *source* but absent in *target*
            (would need to be ``ALTER TABLE … ADD COLUMN`` on target).
        removed: Columns present in *target* but absent in *source*
            (would be dropped in a migration — always breaking).
        type_changed: Pairs of ``(old_col, new_col)`` where the data type
            differs between target and source.
        nullability_changed: Pairs of ``(old_col, new_col)`` where the
            nullability constraint differs.
        is_breaking: ``True`` when any of the following are present:
            column removal, a type change to a narrower type, or a change
            from nullable to not-nullable.
    """

    added: list[ColumnDef] = field(default_factory=list)
    removed: list[ColumnDef] = field(default_factory=list)
    type_changed: list[tuple[ColumnDef, ColumnDef]] = field(default_factory=list)
    nullability_changed: list[tuple[ColumnDef, ColumnDef]] = field(default_factory=list)
    is_breaking: bool = False

    def to_markdown(self) -> str:
        """Render the diff as a Markdown table suitable for a GitHub PR comment.

        Returns:
            A Markdown string. Returns a short notice when no differences exist.
        """
        if not (self.added or self.removed or self.type_changed or self.nullability_changed):
            return "_No schema differences detected._"

        lines: list[str] = []
        breaking_label = " **[BREAKING]**" if self.is_breaking else ""
        lines.append(f"## Schema Diff{breaking_label}\n")

        if self.added:
            lines.append("### Added columns\n")
            lines.append("| Column | Type | Nullable | Default |")
            lines.append("|--------|------|----------|---------|")
            for col in self.added:
                default = col.default or "—"
                lines.append(
                    f"| `{col.name}` | `{col.data_type}` | "
                    f"{'Yes' if col.is_nullable else 'No'} | `{default}` |"
                )
            lines.append("")

        if self.removed:
            lines.append("### Removed columns ⚠️\n")
            lines.append("| Column | Type | Nullable |")
            lines.append("|--------|------|----------|")
            for col in self.removed:
                lines.append(
                    f"| `{col.name}` | `{col.data_type}` | {'Yes' if col.is_nullable else 'No'} |"
                )
            lines.append("")

        if self.type_changed:
            lines.append("### Type changes\n")
            lines.append("| Column | Old type | New type | Breaking? |")
            lines.append("|--------|----------|----------|-----------|")
            for old, new in self.type_changed:
                breaking = "Yes ⚠️" if _is_type_narrowing(old.data_type, new.data_type) else "No"
                lines.append(
                    f"| `{old.name}` | `{old.data_type}` | `{new.data_type}` | {breaking} |"
                )
            lines.append("")

        if self.nullability_changed:
            lines.append("### Nullability changes\n")
            lines.append("| Column | Old | New | Breaking? |")
            lines.append("|--------|-----|-----|-----------|")
            for old, new in self.nullability_changed:
                breaking = "Yes ⚠️" if (old.is_nullable and not new.is_nullable) else "No"
                lines.append(
                    f"| `{old.name}` | "
                    f"{'Nullable' if old.is_nullable else 'NOT NULL'} | "
                    f"{'Nullable' if new.is_nullable else 'NOT NULL'} | "
                    f"{breaking} |"
                )
            lines.append("")

        return "\n".join(lines)

    def to_dict(self) -> dict[str, object]:
        """Serialise the diff to a plain dict for JSON output or CI gating.

        Returns:
            A dict with keys ``added``, ``removed``, ``type_changed``,
            ``nullability_changed``, and ``is_breaking``.
        """

        def col_to_dict(c: ColumnDef) -> dict[str, object]:
            return {
                "name": c.name,
                "data_type": c.data_type,
                "is_nullable": c.is_nullable,
                "default": c.default,
                "comment": c.comment,
            }

        return {
            "added": [col_to_dict(c) for c in self.added],
            "removed": [col_to_dict(c) for c in self.removed],
            "type_changed": [
                {"old": col_to_dict(old), "new": col_to_dict(new)} for old, new in self.type_changed
            ],
            "nullability_changed": [
                {"old": col_to_dict(old), "new": col_to_dict(new)}
                for old, new in self.nullability_changed
            ],
            "is_breaking": self.is_breaking,
        }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_NUMERIC_WIDTH_TYPES = {"VARCHAR", "CHAR", "CHARACTER", "TEXT", "STRING", "BINARY"}


def _extract_type_base_and_size(data_type: str) -> tuple[str, int | None]:
    """Split ``"VARCHAR(256)"`` into ``("VARCHAR", 256)``."""
    data_type = data_type.strip().upper()
    if "(" in data_type:
        base, rest = data_type.split("(", 1)
        try:
            size = int(rest.rstrip(")").split(",")[0])
        except ValueError:
            size = None
        return base.strip(), size
    return data_type, None


def _is_type_narrowing(old_type: str, new_type: str) -> bool:
    """Return True when changing from old_type to new_type is a narrowing change.

    A narrowing change is one that may cause existing data to become invalid,
    e.g. ``VARCHAR(256)`` → ``VARCHAR(64)`` or ``FLOAT`` → ``INTEGER``.

    Args:
        old_type: The original (target table) data type string.
        new_type: The proposed (source table) data type string.

    Returns:
        ``True`` if the change is a type narrowing.
    """
    old_base, old_size = _extract_type_base_and_size(old_type)
    new_base, new_size = _extract_type_base_and_size(new_type)

    if old_base != new_base:
        # Any cross-type change is treated as potentially breaking
        return True

    if old_base in _NUMERIC_WIDTH_TYPES:
        if old_size is not None and new_size is not None:
            return new_size < old_size

    return False


_COLUMNS_QUERY = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    COMMENT
FROM {database}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = %s
  AND TABLE_SCHEMA  = %s
  AND TABLE_NAME    = %s
ORDER BY ORDINAL_POSITION
"""


def _parse_table_ref(table: str) -> tuple[str, str, str]:
    """Parse a fully-qualified table reference into (database, schema, table).

    Args:
        table: A dot-separated table name with exactly three parts.

    Returns:
        Tuple of ``(database, schema, table_name)``, all uppercased.

    Raises:
        SchemaError: If the table reference is not fully qualified.
    """
    parts = [p.strip().strip('"').upper() for p in table.split(".")]
    if len(parts) != 3:
        raise SchemaError(
            f"Table reference '{table}' must be fully qualified as DATABASE.SCHEMA.TABLE_NAME."
        )
    return parts[0], parts[1], parts[2]


def _build_column_def(row: tuple[object, ...]) -> ColumnDef:
    """Construct a ``ColumnDef`` from an ``INFORMATION_SCHEMA.COLUMNS`` row."""
    (
        col_name,
        data_type,
        char_max_len,
        num_precision,
        num_scale,
        is_nullable_str,
        col_default,
        comment,
    ) = row

    # Build a richer type string that includes precision/scale when relevant
    dtype = str(data_type).upper()
    if char_max_len is not None:
        dtype = f"{dtype}({char_max_len})"
    elif num_precision is not None and num_scale is not None:
        dtype = f"{dtype}({num_precision},{num_scale})"
    elif num_precision is not None:
        dtype = f"{dtype}({num_precision})"

    return ColumnDef(
        name=str(col_name),
        data_type=dtype,
        is_nullable=(str(is_nullable_str).upper() == "YES"),
        default=str(col_default) if col_default is not None else None,
        comment=str(comment) if comment else None,
    )


# ---------------------------------------------------------------------------
# SchemaInspector
# ---------------------------------------------------------------------------


class SchemaInspector:
    """Introspects Snowflake table schemas and produces structured diffs.

    Queries ``INFORMATION_SCHEMA.COLUMNS`` — not ``SHOW COLUMNS`` — for
    consistent privilege requirements and stable result formats.

    Args:
        conn: An open ``SnowforgeConnection``.

    Example:
        inspector = SchemaInspector(conn)
        columns = inspector.get_columns("MYDB.PUBLIC.ORDERS")
        diff = inspector.diff("MYDB.STAGING.ORDERS", "MYDB.PUBLIC.ORDERS")
        print(diff.to_markdown())
    """

    def __init__(self, conn: SnowforgeConnection) -> None:
        self._conn = conn

    def get_columns(self, table: str) -> list[ColumnDef]:
        """Fetch column metadata for a fully-qualified Snowflake table.

        Args:
            table: A fully-qualified table name: ``"DATABASE.SCHEMA.TABLE"``.

        Returns:
            Ordered list of ``ColumnDef`` objects matching ``ORDINAL_POSITION``.

        Raises:
            SchemaError: If the table reference is malformed or the query fails.
        """
        database, schema, table_name = _parse_table_ref(table)
        query = _COLUMNS_QUERY.format(database=database)
        try:
            cur = self._conn.execute(query, (database, schema, table_name))
            rows = cur.fetchall()
        except Exception as exc:
            raise SchemaError(f"Failed to fetch column metadata for '{table}': {exc}") from exc

        return [_build_column_def(row) for row in rows]

    def diff(self, source: str, target: str) -> SchemaDiff:
        """Compare the schema of *source* to *target* and return a structured diff.

        The diff is expressed from the perspective of *applying source changes to
        target*:

        * ``added`` — columns in source that target does not have yet.
        * ``removed`` — columns in target that source no longer has.
        * ``type_changed`` — columns whose data type differs.
        * ``nullability_changed`` — columns whose nullable flag differs.

        Args:
            source: Fully-qualified source table (the "new" schema).
            target: Fully-qualified target table (the "current" schema).

        Returns:
            A ``SchemaDiff`` dataclass. ``is_breaking`` is set to ``True`` if
            any column is removed, any type change is a narrowing, or any
            column changes from nullable to not-nullable.

        Raises:
            SchemaError: If either table cannot be introspected.
        """
        source_cols = {c.name: c for c in self.get_columns(source)}
        target_cols = {c.name: c for c in self.get_columns(target)}

        added: list[ColumnDef] = [c for name, c in source_cols.items() if name not in target_cols]
        removed: list[ColumnDef] = [c for name, c in target_cols.items() if name not in source_cols]
        type_changed: list[tuple[ColumnDef, ColumnDef]] = []
        nullability_changed: list[tuple[ColumnDef, ColumnDef]] = []

        for name in source_cols.keys() & target_cols.keys():
            src = source_cols[name]
            tgt = target_cols[name]
            if src.data_type != tgt.data_type:
                type_changed.append((tgt, src))
            if src.is_nullable != tgt.is_nullable:
                nullability_changed.append((tgt, src))

        is_breaking = (
            bool(removed)
            or any(_is_type_narrowing(old.data_type, new.data_type) for old, new in type_changed)
            or any(old.is_nullable and not new.is_nullable for old, new in nullability_changed)
        )

        return SchemaDiff(
            added=added,
            removed=removed,
            type_changed=type_changed,
            nullability_changed=nullability_changed,
            is_breaking=is_breaking,
        )
