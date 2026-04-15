# Changelog

All notable changes to snowcraft will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [0.1.0] — 2026-04-14

### Added

- `SnowforgeConnection`: Context-managed Snowflake connection wrapper with env var fallback.
- `MergeBuilder`: Programmatic `MERGE INTO` statement builder using `sqlglot` for safe identifier quoting. Supports `upsert`, `append`, and `delete_insert` strategies. Watermark-based incremental loading included.
- `SchemaInspector`: Schema introspection via `INFORMATION_SCHEMA.COLUMNS` with structured `SchemaDiff` output (breaking change detection, Markdown and dict serialisation).
- `QueryProfiler`: Surfaces expensive queries and full-table scans from `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`. Includes heuristic optimization hints and cost attribution by warehouse, user, or role.
- `SCDManager`: SCD Type 1 (overwrite) and Type 2 (versioned history with `effective_from` / `effective_to` / `is_current`) helpers.
- Custom exception hierarchy: `SnowforgeError`, `ConnectionError`, `SchemaError`, `MergeError`, `ProfilerError`.
- Full unit test suite (`>90%` coverage) and integration test scaffolding.
- GitHub Actions CI workflow: lint, type-check, and unit tests on Python 3.10–3.12.
- GitHub Actions publish workflow: build and publish to PyPI on tag push using OIDC trusted publishing.
- MkDocs documentation with Material theme and `mkdocstrings` API reference.
