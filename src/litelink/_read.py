"""The three-way read, built per query (SPEC §7).

DuckDB does the reading: pyiceberg resolves the pointer, DuckDB scans, and both
legs of the union run in one engine. `table.scan().to_arrow()` is not used —
its planning happens in Python and costs ~100 ms per scan, paid on every query.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from litelink._types import column_type

if TYPE_CHECKING:
    from collections.abc import Callable

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


def duckdb_connection() -> duckdb.DuckDBPyConnection:
    """A connection with the read path's extensions loaded.

    Provisioned, not autoinstalled — see scripts/install_duckdb_extensions.py
    and §7 on why the first read must not be a network read.

    A module function rather than something `Reader` does for itself, so a
    caller can hand it a different one. It stays a factory rather than a
    connection because building one costs ~140 ms, which a log that only ever
    appends should not pay at `open`.
    """
    connection = duckdb.connect()
    connection.execute("LOAD iceberg")
    # No ATTACH of the buffer database. `Buffer.rows_above` records what that
    # cost: two SQLite libraries in one process is silent corruption, not a
    # slow path.

    return connection


class Reader:
    """A DuckDB connection with the buffer attached, and the union it builds."""

    def __init__(
        self,
        layout: Layout,
        table: LogTable,
        buffer: Buffer,
        schema: pa.Schema,
        connect: Callable[[], duckdb.DuckDBPyConnection],
    ) -> None:
        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._schema = schema
        self._connect_to = connect
        self._connection: duckdb.DuckDBPyConnection | None = None
        # This reader's own, guarding the DuckDB connection and the view built
        # on it. Its own rather than the Log's, because a query must not wait
        # behind a maintenance pass — one waited 21.5 s behind a compaction
        # when the two shared a lock. Reads still serialise against each other:
        # `register` below is connection-global, so two concurrent scans on one
        # connection would swap each other's buffer leg.
        self._lock = threading.Lock()

    def query(self, sql: str) -> pa.RecordBatchReader:
        """Run `sql` against a freshly built `log` relation.

        The relation is rebuilt per call and cannot be held across calls.
        Resolving the table per query is §7's rule, not an optimisation: every
        commit writes a new metadata JSON, so a reader holding the snapshot it
        opened with reports an empty log after the writer's first seal.

        **Each query gets its own cursor**, and that is what makes the returned
        reader safe to hold. A reader is lazy — `_cast_to` keeps it streaming —
        so the caller drains it after this returns. On one shared connection,
        the next query's `register` and `CREATE OR REPLACE TEMP VIEW` land
        underneath a reader still streaming from those same names. Measured: a
        reader over 200 rows returned 0 after another query ran on the
        connection. Not perturbed — destroyed.

        A DuckDB cursor is an independent connection over the same database,
        with its own registrations and temp views, so one query cannot reach
        into another's. Verified directly rather than assumed.
        """
        # Buffer first, table second, and the order is the correctness
        # argument. A seal commits its file and THEN deletes the rows it
        # covered, so between those two moments a row is in both tiers and
        # after them it is only in the table. Reading the buffer first means
        # anything a seal removes afterwards is already in the snapshot
        # resolved below; reading it second would let a seal land in between
        # and leave those rows in neither leg.
        #
        # The floor here only bounds how much is read — §7's point about a
        # deferred delete not inflating a query. It is not the boundary; that
        # is decided after, against a snapshot that cannot then move.
        self._table.reload()
        floor = self._table.extent()
        tail = self._buffer.rows_above(None if floor is None else floor[1])

        with self._lock:
            # The lock covers building the cursor, not the query. Creating one
            # touches the shared connection; running on it does not.
            cursor = self._connect().cursor()

        cursor.register(BUFFER_REL, tail)

        # Resolving per query is §7's rule. Both halves in one call, or a
        # commit between them pairs a new snapshot with an old boundary.
        self._table.reload()
        location, extent = self._table.snapshot()
        # Built every query now rather than cached against its own text. The
        # cache existed to skip reinstalling an identical view on a shared
        # connection; a fresh cursor has no view to reuse, and a CREATE VIEW
        # over an already-registered relation is cheap.
        cursor.execute(
            f"CREATE OR REPLACE TEMP VIEW {VIEW} AS {self._union(location, extent)}"
        )
        reader = cursor.execute(sql).to_arrow_reader()

        return _cast_to(reader, self._schema)

    def _union(self, location: str, extent: tuple[int, int] | None) -> str:
        """The hot read: the local table, plus the buffer above its extent.

        `location` is passed rather than re-read, so the snapshot scanned and
        the boundary cutting the buffer are the same one.
        """
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
        # Applied here as well as pushed into SQLite. The registered tail was
        # read against an earlier floor, so it can still hold rows this
        # snapshot has since taken ownership of; without this they would appear
        # in both legs.
        return (
            f"SELECT {projection} FROM iceberg_scan('{location}')"
            f" UNION ALL SELECT {casts} FROM {BUFFER_REL} b"
            f' WHERE b."{OFFSET}" > {extent[1]}'
        )

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """The connection, built on first read and kept.

        Lazily, so a log that only ever appends never pays for one.
        """
        if self._connection is None:
            self._connection = self._connect_to()

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
