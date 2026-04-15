"""Custom exception hierarchy for snowcraft.

All public-facing errors raised by this library inherit from ``SnowforgeError``
so callers can catch the base class for broad handling or specific subclasses
for fine-grained recovery logic.
"""


class SnowforgeError(Exception):
    """Base exception for all snowcraft errors."""


class ConnectionError(SnowforgeError):
    """Raised when a connection cannot be established or credentials are invalid."""


class SchemaError(SnowforgeError):
    """Raised when schema validation or introspection fails."""


class MergeError(SnowforgeError):
    """Raised when a MERGE statement fails to build or execute."""


class ProfilerError(SnowforgeError):
    """Raised when QUERY_HISTORY access fails, usually due to missing privileges."""
