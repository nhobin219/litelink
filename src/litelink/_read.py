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
    from litelink._layout import Layout
    from litelink._table import LogTable

VIEW = "log"

# The alias the buffer database is attached under.
BUFFER_DB = "buf"

# The library-owned column (§2), which the boundary filters on.
OFFSET = "litelink_offset"


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
        reader = connection.execute(sql).to_arrow_reader()

        return _cast_to(reader, self._schema)

    def _union(self) -> str:
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

        extent = self._table.extent()
        if extent is None:
            # Nothing sealed yet, so there is no boundary to derive and no table
            # to union — every row is still in the buffer.
            return f"SELECT {casts} FROM {self._buffer_source()} b"

        projection = ", ".join(f'"{c}"' for c in columns)

        return (
            f"SELECT {projection} FROM iceberg_scan('{self._table.metadata_location}')"
            f" UNION ALL SELECT {casts} FROM {self._buffer_source(extent[1])} b"
        )

    def _buffer_source(self, boundary: int | None = None) -> str:
        """The buffer leg, with the boundary pushed INTO SQLite.

        Not `FROM buf.buffer WHERE …`. DuckDB's sqlite scanner does not turn
        that predicate into a rowid range, so it reads every buffered row and
        filters above the scan; SQLite given the same predicate answers with
        `SEARCH buffer USING INTEGER PRIMARY KEY (rowid>?)`. Measured over a
        61,000-row buffer returning 1,000: 17.1 ms attached against 1.2 ms
        pushed down, identical results.

        This is what keeps cleanup from costing query latency. Rows sealed but
        not yet deleted are excluded by the boundary either way — only the
        pushed version declines to *read* them, so a deferred step 3 costs disk
        rather than query time and §7's variable cost becomes the UNSEALED rows
        rather than everything the buffer happens to still hold.

        It is also why `binary` columns are unsupported: `sqlite_query` decodes
        blob bytes as UTF-8 and fails. See `_types`.
        """
        columns = ", ".join(f'"{c}"' for c in self._schema.names)
        where = "" if boundary is None else f' WHERE "{OFFSET}" > {boundary}'

        return f"sqlite_query('{BUFFER_DB}', 'SELECT {columns} FROM buffer{where}')"

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._connection is None:
            connection = duckdb.connect()
            # Provisioned, not autoinstalled — see
            # scripts/install_duckdb_extensions.py and §7 on why the first read
            # must not be a network read.
            connection.execute("LOAD iceberg")
            connection.execute("LOAD sqlite")
            connection.execute(
                f"ATTACH '{self._layout.buffer_db}' AS {BUFFER_DB} (TYPE sqlite, READ_ONLY)"
            )
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
