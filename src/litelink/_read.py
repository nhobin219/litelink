"""The three-way read, built per query (SPEC §7).

DuckDB does the reading: pyiceberg resolves the pointer, DuckDB scans, and both
legs of the union run in one engine. `table.scan().to_arrow()` is not used —
its planning happens in Python and costs ~100 ms per scan, paid on every query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from litelink._types import column_type

if TYPE_CHECKING:
    from litelink._buffer import Buffer
    from litelink._layout import Layout
    from litelink._table import LogTable

VIEW = "log"

# The name the buffer's unsealed tail is registered under, re-registered per
# query. NOT an attached database: see `Buffer.rows_above` for why letting
# DuckDB open the SQLite file corrupts it.
BUFFER_REL = "buf_tail"

# The library-owned column (§2), which the boundary filters on.
OFFSET = "litelink_offset"


class Reader:
    """A DuckDB connection with the buffer attached, and the union it builds."""

    def __init__(
        self, layout: Layout, table: LogTable, buffer: Buffer, schema: pa.Schema
    ) -> None:
        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._schema = schema
        self._connection: duckdb.DuckDBPyConnection | None = None
        # The view text last installed. Rebuilding it costs a DuckDB statement
        # per query, and it can only change when the snapshot does.
        self._view: str | None = None

    def query(self, sql: str) -> pa.RecordBatchReader:
        """Run `sql` against a freshly built `log` relation.

        The relation is rebuilt per call and cannot be held across calls.
        Resolving the table per query is §7's rule, not an optimisation: every
        commit writes a new metadata JSON, so a reader holding the snapshot it
        opened with reports an empty log after the writer's first seal.
        """
        self._table.reload()
        connection = self._connect()
        extent = self._table.extent()
        # Re-registered every query, because the buffer has moved on since the
        # last one. Replacing the relation leaves the view valid: the view
        # names it and DuckDB resolves that name at execution.
        connection.register(
            BUFFER_REL, self._buffer.rows_above(None if extent is None else extent[1])
        )
        # Resolving per query is §7's rule and still happens above. What is
        # skipped is reinstalling an identical view: the union text is derived
        # entirely from the metadata pointer and the extent, so unchanged text
        # means an unchanged snapshot.
        union = self._union(extent)
        if union != self._view:
            connection.execute(f"CREATE OR REPLACE TEMP VIEW {VIEW} AS {union}")
            self._view = union

        reader = connection.execute(sql).to_arrow_reader()

        return _cast_to(reader, self._schema)

    def _union(self, extent: tuple[int, int] | None) -> str:
        """The hot read: the local table, plus the buffer above its extent."""
        columns = tuple(self._schema.names)
        # Cast the buffer side explicitly rather than letting UNION ALL
        # reconcile: SQLite's per-value typing comes through loosely, and a
        # column that holds integers in every row can still surprise the union
        # (§7). Aliased back to the bare name, or the buffer-only leg exposes
        # columns called `CAST(b."x" AS BIGINT)`.
        casts = ", ".join(
            f'b."{c}"::{column_type(self._schema.field(c).type).duckdb} AS "{c}"'
            for c in columns
        )

        if extent is None:
            # Nothing sealed yet, so there is no boundary to derive and no table
            # to union — every row is still in the buffer.
            return f"SELECT {casts} FROM {BUFFER_REL} b"

        projection = ", ".join(f'"{c}"' for c in columns)

        return (
            f"SELECT {projection} FROM iceberg_scan('{self._table.metadata_location}')"
            f" UNION ALL SELECT {casts} FROM {BUFFER_REL} b"
        )

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            connection = duckdb.connect()
            # Provisioned, not autoinstalled — see
            # scripts/install_duckdb_extensions.py and §7 on why the first read
            # must not be a network read.
            connection.execute("LOAD iceberg")
            # No ATTACH of the buffer database. `Buffer.rows_above` records
            # what that cost: two SQLite libraries in one process is silent
            # corruption, not a slow path.
            self._connection = connection

        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


def _cast_to(reader: pa.RecordBatchReader, schema: pa.Schema) -> pa.RecordBatchReader:
    """Cast a reader's batches to the declared column types — the DuckDB edge.

    DuckDB has one string type and one blob type and returns the 32-bit-offset
    Arrow forms for both, so a column declared `large_binary` would otherwise
    come back as `binary`: a silent contradiction of what the caller asked for.

    Lazy, so a streaming read stays streaming. Nearly free — widening offsets
    shares the data buffer, measured at 0.03 ms for 50,000 rows over 21 MB —
    which is why this is done on every batch rather than only when it differs.

    A projection selects a subset of columns, so the target is narrowed to
    whatever the query actually returned.
    """
    target = pa.schema(
        [
            schema.field(name) if name in schema.names else reader.schema.field(name)
            for name in reader.schema.names
        ]
    )
    if target.equals(reader.schema):
        return reader

    return pa.RecordBatchReader.from_batches(
        target, (batch.cast(target) for batch in reader)
    )
