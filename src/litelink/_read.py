"""The three-way read, built per query (SPEC §7).

DuckDB does the reading: pyiceberg resolves the pointer, DuckDB scans, and both
legs of the union run in one engine. `table.scan().to_arrow()` is not used —
its planning happens in Python and costs ~100 ms per scan, paid on every query.
"""

from __future__ import annotations

import contextlib
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa

from litelink._archive import Archive
from litelink._s3 import S3Options
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


def secret_sql(options: S3Options) -> str:
    """DuckDB's spelling of the same credentials pyiceberg was given.

    Separate from the query path so it can be tested without object storage —
    the fault it exists to prevent only appeared against real AWS, where writes
    worked and every `include_archive` read came back 403.
    """
    parts = ["TYPE s3"]
    if options.access_key is not None and options.secret_key is not None:
        parts.append(f"KEY_ID '{options.access_key}'")
        parts.append(f"SECRET '{options.secret_key}'")
    else:
        # Nothing explicit means the credentials live where every AWS tool
        # looks for them: a profile, instance metadata, SSO. pyiceberg and s3fs
        # resolve those themselves, so WRITES worked while reads got a secret
        # with no keys — which DuckDB treats as anonymous, so a log on an
        # ordinary AWS host archived happily and answered every archive read
        # with 403. A local endpoint never showed it, because there the keys
        # are always passed explicitly.
        #
        # `credential_chain` is DuckDB's equivalent of that resolution. The
        # else-branch rather than the default, because an explicit key must
        # still win — see `S3Options.resolved`.
        parts.append("PROVIDER credential_chain")

    if options.region is not None:
        parts.append(f"REGION '{options.region}'")

    if options.endpoint is not None:
        # DuckDB wants host:port with the scheme carried by USE_SSL, unlike
        # pyiceberg which takes a URL. One S3Options, two spellings.
        without_scheme = options.endpoint.split("://", 1)[-1]
        parts.append(f"ENDPOINT '{without_scheme}'")
        parts.append(f"USE_SSL {str(options.endpoint.startswith('https')).lower()}")
        parts.append("URL_STYLE 'path'")

    return f"CREATE OR REPLACE SECRET litelink_s3 ({', '.join(parts)})"


class ExtensionMissing(RuntimeError):
    """A DuckDB extension the read path needs is not on this machine (§7)."""


def _bundled_extension(connection: duckdb.DuckDBPyConnection, name: str) -> Path | None:
    """The copy shipped in the wheel, if there is one this DuckDB can load.

    **Checked against the RUNNING version and platform, never assumed.** DuckDB
    builds extensions per version and per platform — the download path is
    `/v1.5.5/linux_amd64/...` — so a bundle is only usable by the exact DuckDB
    it was fetched for. `duckdb>=1.5.5` has no ceiling, so a user can perfectly
    well resolve a newer one than the wheel was built against, and loading a
    mismatched extension is not something to find out at read time.

    Returning None on a miss is deliberate: the caller falls through to the
    ordinary `LOAD`, which finds a machine-provisioned copy if there is one and
    otherwise raises the message that says how to get one. The bundle is a fast
    path, not the mechanism.
    """
    root = Path(__file__).resolve().parent / ".bin"
    if not root.is_dir():
        return None

    platform = connection.execute("PRAGMA platform").fetchone()
    if platform is None:
        return None

    candidate = (
        root / duckdb.__version__ / str(platform[0]) / f"{name}.duckdb_extension"
    )

    return candidate if candidate.is_file() else None


def load_extension(
    connection: duckdb.DuckDBPyConnection, name: str, *, remote: bool
) -> None:
    """`LOAD name`, with an error that says what to do about it.

    **LOAD-time autoinstall is per-extension, and not to be relied on either
    way.** Verified against duckdb 1.5.5, each in a fresh process with an empty
    `extension_directory` and `autoinstall_known_extensions` reporting True:
    `LOAD iceberg` silently downloads and succeeds, `LOAD httpfs` raises. So
    the reported failure is not a machine with autoinstall switched off — the
    two extensions simply behave differently, and nothing here should assume
    which.

    What the caller got instead was DuckDB's error: a filesystem path inside
    `~/.duckdb`, and advice to run `INSTALL httpfs` — a remedy §7 argues
    against, naming nothing this repo ships. `iceberg` reaches the same error
    the moment the machine is genuinely offline, which is the case §7 is about,
    so both go through here.

    **Installing here instead is the other way to make that traceback go away,
    and it is refused.** §7 makes provisioning an obligation discharged at
    build or deploy time, and `install_duckdb_extensions.py --check` exists to
    assert a machine has discharged it. A read path that quietly downloads is
    precisely what that check is written to detect, so adding one would leave
    the check passing on a machine it was meant to fail.

    `remote` says whether this extension is needed only for the archive tier,
    which changes what the message should tell the reader: a local-first log
    never loads `httpfs`, so someone who hits it has either configured an
    archive or is reading one, and someone who has not can ignore it entirely.
    It also picks the flag for the contributor recipe.
    """
    bundled = _bundled_extension(connection, name)
    if bundled is not None:
        connection.execute(f"LOAD '{bundled}'")

        return

    try:
        connection.execute(f"LOAD {name}")
    except duckdb.IOException as exc:
        flag = " --remote" if remote else ""
        tier = (
            "\n\nOnly the archive tier needs this. A local-first log never "
            "loads it, so if you are not reading an archive you can ignore it."
            if remote
            else ""
        )
        msg = (
            f"the DuckDB `{name}` extension is not installed on this machine. "
            f"It is not compiled into the duckdb wheel, and an explicit LOAD "
            f"does not fetch it, so it has to be provisioned:\n"
            f"\n"
            f"    pip install 'litelink[archive]'   # ships it for duckdb "
            f"{duckdb.__version__}\n"
            f"\n"
            f"or fetch it into this machine's DuckDB home, which needs "
            f"network:\n"
            f"\n"
            f'    python -c "import duckdb; '
            f"duckdb.connect().execute('INSTALL {name}')\"\n"
            f"\n"
            f"Extensions are built per DuckDB version and platform, so one "
            f"fetched for a different duckdb will not load."
            f"\n\nFrom a checkout:    just duckdb-extensions{flag}"
            f"{tier}"
        )
        raise ExtensionMissing(msg) from exc


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
    # `avro` BEFORE `iceberg`, and quietly: `iceberg`'s init auto-installs it
    # otherwise, which needs the network and defeats the point of bundling.
    # Missing is not fatal here — a machine-provisioned `iceberg` may resolve
    # it however it already does — so the failure to report is `iceberg`'s.
    with contextlib.suppress(ExtensionMissing, duckdb.Error):
        load_extension(connection, "avro", remote=False)

    load_extension(connection, "iceberg", remote=False)
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
        connect: Callable[[], duckdb.DuckDBPyConnection],
        archive: Archive,
    ) -> None:
        self._layout = layout
        self._table = table
        self._buffer = buffer
        self._connect_to = connect
        # The shared archive object, not a table handle: it opens the table on
        # first use, so a reader that never passes `include_archive` never
        # touches the network (I5), and `Log.set_archive` re-points this same
        # object rather than leaving the reader holding a stale one.
        self._archive = archive
        self._remote_ready = False
        self._connection: duckdb.DuckDBPyConnection | None = None
        # This reader's own, guarding the DuckDB connection and the view built
        # on it. Its own rather than the Log's, because a query must not wait
        # behind a maintenance pass — one waited 21.5 s behind a compaction
        # when the two shared a lock. Reads still serialise against each other:
        # `register` below is connection-global, so two concurrent scans on one
        # connection would swap each other's buffer leg.
        self._lock = threading.Lock()

    @property
    def _schema(self) -> pa.Schema:
        """Read, never remembered — a reader opened before a schema change
        must not serve the old columns for the rest of its life.

        `.table`, not `.schema`: this is the shape the DuckDB view is built
        in, and every projection here names `litelink_offset` first.
        """
        return self._buffer.shape().table

    def _prepare_remote(
        self, cursor: duckdb.DuckDBPyConnection
    ) -> tuple[str, tuple[int, int]] | None:
        """Load httpfs, install the credentials, return the archive's pointer
        and the offset range it covers.

        None when there is no archive or it holds nothing yet, which is the
        signal to build the union without a third leg rather than to fail.

        The extent comes back with the pointer because the union needs it when
        there is nothing sealed locally: with no local extent there is no
        boundary between the archive and the buffer, and the registered tail
        can hold rows the archive has since taken ownership of.
        """
        archive = self._archive.table()
        if archive is None:
            return None

        archive.reload()
        # Paired, not two reads. `snapshot()` exists because a pointer and an
        # extent taken separately can come from different commits — and this
        # handle is shared, so a concurrent `sync` advances it between them.
        # Unpaired, the empty-extent branch below scans a NEWER archive
        # snapshot while cutting the buffer at an OLDER extent, and every row
        # registered in between comes back from both legs.
        location, covered = archive.snapshot()
        if covered is None:
            return None

        if not self._remote_ready:
            # Once per connection. `httpfs` is not in the local read path, so a
            # log that never opts in never pays for it — §7's rule that a hot
            # read is offline.
            load_extension(self._connect(), "httpfs", remote=True)
            self._remote_ready = True

        cursor.execute(secret_sql(self._archive.s3.resolved()))

        return location, covered

    def query(self, sql: str, *, include_archive: bool = False) -> pa.RecordBatchReader:
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

        # The archive AFTER the local snapshot, and that order is the
        # correctness argument. The archive leg is bounded by `extent[0]` — the
        # oldest offset still local — so every offset below it must be in the
        # archive snapshot this reads. Resolved first, it can be older than the
        # bound it is measured against: a sync registering [100, 199] and an
        # eviction dropping them both land in between, and the reader pairs an
        # archive snapshot ending at 99 with a local extent starting at 200.
        # Rows 100-199 are then in no leg at all.
        #
        # After, it cannot happen. I4 means nothing is evicted before it is
        # registered, so an archive snapshot taken later than the local one
        # holds everything the local one has given up.
        remote = self._prepare_remote(cursor) if include_archive else None
        # Built every query now rather than cached against its own text. The
        # cache existed to skip reinstalling an identical view on a shared
        # connection; a fresh cursor has no view to reuse, and a CREATE VIEW
        # over an already-registered relation is cheap.
        cursor.execute(
            f"CREATE OR REPLACE TEMP VIEW {VIEW} AS "
            f"{self._union(location, extent, remote)}"
        )
        reader = cursor.execute(sql).to_arrow_reader()

        return _cast_to(reader, self._schema)

    def _union(
        self,
        location: str,
        extent: tuple[int, int] | None,
        remote: tuple[str, tuple[int, int]] | None = None,
    ) -> str:
        """The hot read: the local table, plus the buffer above its extent.

        `location` is passed rather than re-read, so the snapshot scanned and
        the boundary cutting the buffer are the same one.
        """
        # Bound once. `_schema` reads through the buffer now, so touching it
        # inside the comprehension below would be a keyed `meta` read per
        # COLUMN rather than per query.
        schema = self._schema
        columns = tuple(schema.names)
        # Cast the buffer side explicitly rather than letting UNION ALL
        # reconcile: SQLite's per-value typing comes through loosely, and a
        # column that holds integers in every row can still surprise the union
        # (§7). Aliased back to the bare name, or the buffer-only leg exposes
        # columns called `CAST(b."x" AS BIGINT)`.
        casts = ", ".join(
            f'b."{c}"::{column_type(schema.field(c).type).duckdb} AS "{c}"'
            for c in columns
        )

        projection = ", ".join(f'"{c}"' for c in columns)
        buffered = f"SELECT {casts} FROM {BUFFER_REL} b"
        if extent is None:
            # Nothing sealed locally. The archive can still hold history — a
            # log evicted down to nothing is §8's `local_retention=0` shape —
            # and then everything local is in the buffer.
            if remote is None:
                return buffered

            # The buffer still needs a floor, and with no local extent the
            # archive supplies it. The tail was registered against an earlier
            # read, so it can hold rows that have since been sealed, archived
            # and evicted — leaving the local table empty again and both legs
            # claiming them. Bounding here is what the `extent[1]` cut does in
            # the branch below, from the only boundary available.
            location, covered = remote

            return (
                f"SELECT {projection} FROM iceberg_scan('{location}')"
                f' UNION ALL {buffered} WHERE b."{OFFSET}" > {covered[1]}'
            )

        legs = []
        if remote is not None:
            # Strictly below what the local table holds. `extent[0]` is the
            # oldest offset still local, so anything at or above it is served
            # from disk rather than over the network.
            legs.append(
                f"SELECT {projection} FROM iceberg_scan('{remote[0]}')"
                f' WHERE "{OFFSET}" < {extent[0]}'
            )

        legs.append(f"SELECT {projection} FROM iceberg_scan('{location}')")
        # The buffer bound is applied here as well as pushed into SQLite. The
        # registered tail was read against an earlier floor, so it can still
        # hold rows this snapshot has since taken ownership of; without this
        # they would appear in both legs.
        legs.append(f'{buffered} WHERE b."{OFFSET}" > {extent[1]}')

        return " UNION ALL ".join(legs)

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
