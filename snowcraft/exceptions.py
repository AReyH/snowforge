"""Custom exception hierarchy for snowcraft.

All public-facing errors raised by this library inherit from ``SnowcraftError``
so callers can catch the base class for broad handling or specific subclasses
for fine-grained recovery logic.
"""


class SnowcraftError(Exception):
    """Base exception for all snowcraft errors."""


class ConnectionError(SnowcraftError):
    """Raised when a connection cannot be established or credentials are invalid."""


class SchemaError(SnowcraftError):
    """Raised when schema validation or introspection fails."""


class MergeError(SnowcraftError):
    """Raised when a MERGE statement fails to build or execute."""


class ProfilerError(SnowcraftError):
    """Raised when QUERY_HISTORY access fails, usually due to missing privileges."""
