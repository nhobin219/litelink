"""The three-way read, built per query (SPEC §7).

DuckDB does the reading: pyiceberg resolves the pointer, DuckDB scans, and both
legs of the union run in one engine. `table.scan().to_arrow()` is not used —
its planning happens in Python and costs ~100 ms per scan, paid on every query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb

from litelink._types import column_type

if TYPE_CHECKING:
    import pyarrow as pa

    from litelink._layout import Layout
    from litelink._table import LogTable

VIEW = "log"


class Reader:
    """A DuckDB connection with the buffer attached, and the union it builds."""

    def __init__(self, layout: Layout, table: LogTable, schema: pa.Schema) -> None:
        self._layout = layout
        self._table = table
        self._schema = schema
        self._connection: duckdb.DuckDBPyConnection | None = None

    def query(self, sql: str) -> pa.RecordBatchReader:
        """Run `sql` against a freshly built `log` relation.

        The relation is rebuilt per call and cannot be held across calls.
        Resolving the table per query is §7's rule, not an optimisation: every
        commit writes a new metadata JSON, so a reader holding the snapshot it
        opened with reports an empty log after the writer's first seal.
        """
        self._table.reload()
        connection = self._connect()
        connection.execute(f"CREATE OR REPLACE TEMP VIEW {VIEW} AS {self._union()}")

        return connection.execute(sql).to_arrow_reader()

    def _union(self) -> str:
        """The hot read: the local table, plus the buffer above its extent."""
        columns = tuple(self._schema.names)
        # Cast the buffer side explicitly rather than letting UNION ALL
        # reconcile: SQLite's per-value typing comes through the scanner
        # loosely, and a column that holds integers in every row can still
        # surprise the union (§7).
        #
        # Aliased back to the bare name: without it the buffer-only leg
        # (nothing sealed yet) exposes columns called `CAST(b."offset" AS
        # BIGINT)`, and only an Iceberg leg naming them first hides it.
        buffer_leg = (
            "SELECT "
            + ", ".join(
                f'b."{c}"::{column_type(self._schema.field(c).type).duckdb} AS "{c}"'
                for c in columns
            )
            + " FROM buf.buffer b"
        )

        extent = self._table.extent()
        if extent is None:
            # Nothing sealed yet, so there is no boundary to derive and no table
            # to union — every row is still in the buffer.
            return buffer_leg

        projection = ", ".join(f'"{c}"' for c in columns)

        return (
            f"SELECT {projection} FROM iceberg_scan('{self._table.metadata_location}')"
            f' UNION ALL {buffer_leg} WHERE b."offset" > {extent[1]}'
        )

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            connection = duckdb.connect()
            # Provisioned, not autoinstalled — see
            # scripts/install_duckdb_extensions.py and §7 on why the first read
            # must not be a network read.
            connection.execute("LOAD iceberg")
            connection.execute("LOAD sqlite")
            connection.execute(
                f"ATTACH '{self._layout.buffer_db}' AS buf (TYPE sqlite, READ_ONLY)"
            )
            self._connection = connection

        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
