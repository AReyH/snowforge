"""snowcraft — the Snowflake standard library that never shipped.

Provides clean, typed, well-tested wrappers around the most common Snowflake
operations so data engineers can stop copy-pasting boilerplate and start
importing.

Public API surface
------------------

Connection
~~~~~~~~~~
.. code-block:: python

    from snowcraft import SnowcraftConnection

    with SnowcraftConnection() as conn:
        conn.execute("SELECT CURRENT_USER()")

MERGE / Incremental loads
~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    from snowcraft import MergeBuilder, MergeResult

    result: MergeResult = MergeBuilder(
        conn=conn,
        target_table="MYDB.PUBLIC.ORDERS",
        source_query="SELECT order_id, status, updated_at FROM MYDB.STAGING.ORDERS",
        match_keys=["order_id"],
    ).execute()

Schema inspection and diffing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    from snowcraft import SchemaInspector, SchemaDiff

    diff: SchemaDiff = SchemaInspector(conn).diff(
        source="MYDB.STAGING.ORDERS",
        target="MYDB.PUBLIC.ORDERS",
    )
    print(diff.to_markdown())

Query profiling
~~~~~~~~~~~~~~~
.. code-block:: python

    from snowcraft import QueryProfiler

    for q in QueryProfiler(conn).top_expensive(n=10):
        print(q.query_id, q.optimization_hints)

Slowly Changing Dimensions
~~~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    from snowcraft import SCDManager

    SCDManager(
        conn=conn,
        target_table="MYDB.DW.DIM_CUSTOMER",
        source_query="SELECT customer_id, name, email FROM MYDB.STAGING.CUSTOMERS",
        business_keys=["customer_id"],
        tracked_columns=["name", "email"],
    ).apply_type2()

Exceptions
~~~~~~~~~~
.. code-block:: python

    from snowcraft.exceptions import SnowcraftError, MergeError
"""

from snowcraft.connection import SnowcraftConnection
from snowcraft.exceptions import (
    ConnectionError,
    MergeError,
    ProfilerError,
    SchemaError,
    SnowcraftError,
)
from snowcraft.merge import MergeBuilder, MergeResult
from snowcraft.profiler import CostSummary, QueryProfiler, QuerySummary
from snowcraft.scd import SCDManager, SCDResult
from snowcraft.schema import ColumnDef, SchemaDiff, SchemaInspector

__all__ = [
    # Connection
    "SnowcraftConnection",
    # Merge
    "MergeBuilder",
    "MergeResult",
    # Schema
    "SchemaInspector",
    "SchemaDiff",
    "ColumnDef",
    # Profiler
    "QueryProfiler",
    "QuerySummary",
    "CostSummary",
    # SCD
    "SCDManager",
    "SCDResult",
    # Exceptions
    "SnowcraftError",
    "ConnectionError",
    "SchemaError",
    "MergeError",
    "ProfilerError",
]

__version__ = "0.1.0"
__author__ = "Arturo Rey"
