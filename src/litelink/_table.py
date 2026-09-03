"""The local Iceberg table, and the pyiceberg calls that reach it.

Everything that knows pyiceberg's shape lives here, so the rest of the library
deals in offsets, paths and extents. Several methods exist only because
pyiceberg's own behaviour needed working around — each says which.
"""

from __future__ import annotations

import random
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.conversions import from_bytes
from pyiceberg.exceptions import (
    CommitFailedException,
    NoSuchTableError,
)
from pyiceberg.io import load_file_io
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import StaticTable
from pyiceberg.transforms import IdentityTransform

from litelink._fs import fsync
from litelink._predicates import offset_at_or_below, offset_between
from litelink._s3 import S3Options

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from datetime import datetime
    from pathlib import Path

    import pyarrow as pa
    from pyiceberg.io import FileIO
    from pyiceberg.table import Table
    from pyiceberg.table.snapshots import Snapshot

    from litelink._layout import Layout

# Iceberg keeps every metadata.json ever written unless told otherwise, which on
# a stream that seals every few minutes is a file per seal, forever. These bound
# it to the current version plus a few for rollback. Manifests are NOT covered —
# see `expire_snapshots`.
# Bounded: a commit that keeps losing is contention worth surfacing, not
# something to retry forever behind a caller's back.
# Eight, not five. Every loser of a CAS reloads and re-commits, so under real
# contention — a writer sealing while two maintainers commit disjoint ranges —
# five immediate attempts were exhausted in a 45 s run. The cost of an extra
# attempt is one reload; the cost of exhausting them is a pass that did its
# whole rewrite and threw the result away.
_COMMIT_ATTEMPTS = 8
# The first backoff ceiling, doubling per attempt. Small enough that an
# uncontended retry is imperceptible, and by the last attempt wide enough that
# several committers land on different milliseconds.
_COMMIT_BACKOFF_MS = 20

METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
    # Each commit otherwise writes its own manifest, and litelink commits one
    # file per seal — so a table of N data files becomes N manifest avro files,
    # and deriving the tier boundary means opening every one of them. Measured
    # at 60 files: 60 manifests and a 45 ms boundary read, against 1 manifest
    # and 2.3 ms with merging on.
    #
    # It is not a trade against write cost, which is what makes the choice easy.
    # Accumulated manifests slow every later commit too, since each rewrites a
    # manifest list naming all of them: the same 60 seals ran at a 110 ms median
    # unmerged and 65 ms merged.
    #
    # min-count 2 merges on essentially every commit, which is as close as this
    # property gets to the one manifest a stream actually wants. Iceberg's
    # default is 100 — sized for batch jobs, and hours of accumulation for a
    # stream that seals every few minutes.
    "commit.manifest-merge.enabled": "true",
    "commit.manifest.min-count-to-merge": "2",
}


@dataclass(frozen=True, slots=True)
class DataFile:
    """One Iceberg data file, as the maintenance passes need to see it."""

    path: str
    size: int
    rows: int
    lo: int
    hi: int


# The catalog name the archive's `SqlCatalog` is built with, and the key its
# rows are stored under. One constant so the reader below and the writer above
# cannot disagree about which rows are ours.
ARCHIVE_CATALOG = "archive"

# The local catalog's name, spelled once. `exists_for` reads `iceberg_tables`
# by it directly, so a literal here and a different one in `_catalog_for` would
# make that read answer False for every table that exists.
LOCAL_CATALOG = "local"


class ArchiveAbsent(LookupError):
    """No archive table exists at the prefix yet.

    Distinct from a mismatch, because the two want opposite handling: absent is
    the ordinary state of a log whose first sync has not run, and a reader
    simply leaves that leg out of the union. A mismatch means the catalog names
    somewhere the log is not pointed, which no reader should quietly work
    around.
    """


def _recorded_location(layout: Layout) -> str | None:
    """Where the archive's catalog entry says its metadata is, without a read.

    Straight out of the catalog's own SQLite file, because the question — does
    this entry belong to the prefix being opened? — has to be answerable when
    the bucket it names is unreachable. `load_table` would fetch the metadata
    to tell us, and that fetch is exactly the one that can fail for reasons
    other than "there is no table".

    None means there is definitely no entry — the catalog was readable and had
    no row. Callers distinguish that from "cannot tell", so the two must not be
    conflated: the empty string is used below as a third value meaning "carry
    on and let pyiceberg decide". `LookupError` means the question could not be answered, which is
    NOT the same thing: answering None there sends the caller down the create
    path against an entry that still exists, and every open of that log then
    fails on a unique constraint. A log nobody can open, from a query that was
    only ever an optimisation.

    The schema is pyiceberg's, not ours: table `iceberg_tables`, keyed by
    catalog name, namespace and table name. Verified against a real catalog
    rather than assumed. If a future version changes it, the query fails, the
    caller falls back to loading, and pyiceberg answers for itself — slower and
    correct, instead of fast and destructive.
    """
    if not layout.archive_db.exists():
        return None

    namespace, _, name = layout.table_id.rpartition(".")
    connection = sqlite3.connect(f"file:{layout.archive_db}?mode=ro", uri=True)
    try:
        row = connection.execute(
            "SELECT metadata_location FROM iceberg_tables"
            " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?",
            (ARCHIVE_CATALOG, namespace, name),
        ).fetchone()
    except sqlite3.Error as exc:
        msg = f"cannot read the archive catalog's own table: {exc}"
        raise LookupError(msg) from exc
    finally:
        connection.close()

    return None if row is None or row[0] is None else str(row[0])


# Where an archive says which of its metadata JSONs is current, written beside
# them at every commit. `SqlCatalog` keeps that pointer in the catalog rather
# than in the warehouse (§7), so without this the local `archive.db` is the
# ONLY thing that names the archive's current metadata — and `open_archive`
# says what that costs: a re-point drops the row, and "a drop that is not
# followed by a create destroys the only record of where the PREVIOUS
# archive's metadata is."
#
# The name is the one DuckDB's iceberg extension looks for, so a reader can
# scan the directory rather than being handed a pointer:
#
#     iceberg_scan(dir, version_name_format = '%s%s.metadata.json')
#
# **That parameter is needed, and it is the cost of not copying anything.**
# DuckDB's default format is `v%s%s.metadata.json`, the Hadoop convention, and
# pyiceberg names its metadata `00003-<uuid>.metadata.json` — so the hint holds
# that stem and the format has to stop prepending a `v`. Both spellings were
# verified against duckdb 1.5.5.
#
# **Rejected: the Hadoop convention, by copying.** Writing the metadata a
# second time as `v3.metadata.json` and putting `3` in the hint reads with
# DuckDB's defaults and with anything else expecting a Hadoop table — verified
# too. It also adds an object per commit that nothing collects: deleting the
# previous one races a reader between its hint read and its metadata read,
# which is the race `pending_delete` exists to avoid for snapshots and would
# have to be extended to cover. Growth with no collector, or a new class of
# deletion to sequence — against one parameter in one documented query.
VERSION_HINT = "version-hint.text"


def _hint_for(metadata_location: str) -> tuple[str, str]:
    """Where the hint goes for a metadata pointer, and what it should say."""
    directory, _, name = metadata_location.rpartition("/")

    return f"{directory}/{VERSION_HINT}", name.removesuffix(".metadata.json")


def _published_location(io: FileIO, layout: Layout, prefix: str) -> str | None:
    """What the archive at `prefix` says its current metadata is, or None.

    Read from the bucket, which is the point: it answers for an archive this
    log has no catalog row for. The directory is reconstructed from the layout
    — `{prefix}/{name}/metadata` — rather than from a table handle, because
    there is no handle yet; that is the situation.

    It must be reconstructed the SAME way `create_table` is told to lay the
    archive out. It once was not: this rebuilt pyiceberg's default
    `{prefix}/{namespace}/{name}/metadata` while the table location had been
    set explicitly to `{prefix}/{name}`, so `publish_pointer` — which derives
    its path from the live metadata location — wrote the hint to one place and
    this looked for it in another. Every write was correct and every read said
    ABSENT, which is the one answer that makes the caller create an empty table
    over a live archive.

    **None means ABSENT, never "could not tell".** An earlier version swallowed
    every failure into None, and the caller reads None as "nothing here yet,
    create one" — so one 503, one expired token, one throttled GET on this
    single object was indistinguishable from a virgin prefix, and the repair
    built an empty table over a live archive. Then `publish_pointer`
    republished the hint onto the empty lineage, destroying the last pointer to
    the real one; after a re-point has dropped the catalog row, that hint is
    all there is. The sibling `load_table` branch refuses exactly this, in
    those words.

    So a read that fails RAISES. Callers that would rather carry on — the
    `set_archive` guard, which must not fail closed on a bad minute in object
    storage — catch it themselves and say so.
    """
    directory = f"{layout.archive_table_location(prefix)}/metadata"
    source = io.new_input(f"{directory}/{VERSION_HINT}")
    if not source.exists():
        return None

    version = source.open().read().decode().strip()

    return f"{directory}/{version}.metadata.json" if version else None


def forget_archive_entry(layout: Layout) -> bool:
    """Drop the archive's catalog row, so the next open must ADOPT.

    For a restore: a row that survived onto this machine describes an archive
    as it was when the replica was taken, and `open_archive` reads
    `version-hint.text` only when there is no row. A stale row therefore wins
    over the bucket's own pointer, silently and in the losing direction.

    Returns whether a row was there. Touches the table `SqlCatalog` owns
    directly rather than through pyiceberg, because constructing a catalog to
    drop one row would create the catalog's own tables as a side effect — and
    on a fresh restore there is no `archive.db` at all, which is the ordinary
    case and must stay a no-op.
    """
    if not layout.archive_db.exists():
        return False

    namespace, _, name = layout.table_id.rpartition(".")
    connection = sqlite3.connect(layout.archive_db)
    try:
        cursor = connection.execute(
            "DELETE FROM iceberg_tables"
            " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?",
            (ARCHIVE_CATALOG, namespace, name),
        )
        connection.commit()
    except sqlite3.Error:
        # No such table means no entry, which is what this wanted anyway.
        return False
    finally:
        connection.close()

    return bool(cursor.rowcount)


def archive_extent(
    layout: Layout, prefix: str, options: S3Options
) -> tuple[int, int] | None:
    """`(lo, hi)` of the archive at `prefix`, read from the bucket alone.

    Answers "what does that archive hold" WITHOUT touching `archive.db`, which
    is what makes it usable as a pre-flight check. The catalog row is keyed by
    table id, so while a log is pointed at one archive the row names that one —
    `open_archive` on a different prefix therefore either raises on the boundary
    check or, with `repair=True`, drops the row as a side effect of a read.

    Two objects instead: the published pointer, then the metadata it names.
    `None` when the archive has no hint, which covers both "nothing has been
    pushed there" and "it is not a litelink archive". A hint that cannot be
    READ raises instead, because `_published_location` no longer conflates the
    two — the caller decides whether a bad minute in object storage is fatal,
    and `_refuse_archive_ahead` treats it as "cannot tell" and passes.
    """
    io = load_file_io(options.resolved().catalog_properties(), prefix)
    location = _published_location(io, layout, prefix)
    if location is None:
        return None

    table = StaticTable.from_metadata(location, options.resolved().catalog_properties())

    return LogTable(None, layout, table, prefix).extent()  # ty: ignore


def archive_shape(
    layout: Layout, prefix: str, options: S3Options
) -> tuple[pa.Schema, tuple[str, ...]] | None:
    """The archive's declared shape, read from the bucket alone (§3b).

    What an archive-only snapshot needs and cannot get anywhere else. `follow`
    reads the schema, the sort order and the archive's location from the
    REPLICA's `meta`, which is authoritative — it is the writer's own copy, and
    it survives a re-point. A snapshot that skips the WAL has no replica, so
    the only thing left that knows the shape is the archive's own Iceberg
    metadata, and the only thing that says where the archive is, is the caller.

    That asymmetry is the reason `include_wal=False` is not simply a faster
    path to the same answer, and `snapshot` says so.

    `None` when the archive has no published hint — nothing pushed there, or
    not a litelink archive. The caller cannot tell those apart and must not
    guess: `archive_extent` draws the same line for the same reason.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    io = load_file_io(options.resolved().catalog_properties(), prefix)
    location = _published_location(io, layout, prefix)
    if location is None:
        return None

    table = StaticTable.from_metadata(location, options.resolved().catalog_properties())
    names = {f.field_id: f.name for f in table.schema().fields}
    sort_by = tuple(names[f.source_id] for f in table.sort_order().fields)

    # **From a DATA FILE's footer, not from the Iceberg schema**, and the
    # difference is observable. Iceberg has one string type, so a `string`
    # column comes back from `schema().as_arrow()` as `large_string` — while
    # every other path a caller can see reports `string`, because the Parquet
    # is written from the DECLARED Arrow schema and the footer carries it
    # verbatim. That footer is what DuckDB reads, which is why a local read and
    # an archived read agree today; taking the schema from Iceberg here would
    # have made this the one path that disagreed with `scan()` on its own
    # handle.
    #
    # One extra GET, against a path whose entire purpose is avoiding twenty.
    # Read through the FileIO already built above rather than a second
    # filesystem: `InputFile.open()` is seekable, which is all a footer needs.
    for task in table.scan().plan_files():
        with io.new_input(task.file.file_path).open() as data:
            written = pq.read_schema(data)

        return (
            pa.schema([f for f in written if f.name != "litelink_offset"]),
            sort_by,
        )

    # Published metadata naming no data file. Nothing can be served from it, so
    # the schema is academic — but answering with Iceberg's projection would be
    # the disagreement above, so it says so instead.
    return pa.schema(
        [f for f in table.schema().as_arrow() if f.name != "litelink_offset"]
    ), sort_by


def archive_columns(
    layout: Layout, prefix: str, options: S3Options
) -> tuple[str, ...] | None:
    """The column names of the archive at `prefix`, read from the bucket alone.

    The sibling of `archive_extent` and for the same reason: a pre-flight
    check has to ask about an archive this log is not pointed at yet, and
    `open_archive` on a different prefix either raises on the boundary check
    or drops a catalog row as a side effect.

    `None` when the archive has no published hint, which covers both "nothing
    has been pushed there" and "not a litelink archive" — neither of which the
    caller can conclude anything from.
    """
    io = load_file_io(options.resolved().catalog_properties(), prefix)
    location = _published_location(io, layout, prefix)
    if location is None:
        return None

    table = StaticTable.from_metadata(location, options.resolved().catalog_properties())

    return tuple(field.name for field in table.schema().fields)


class LogTable:
    """The local Iceberg table for one log.

    Holds a pyiceberg `Table`, which is a snapshot-in-time view, and reloads it
    whenever the current state matters. §7 is explicit that a cached pointer
    silently serves a stale snapshot.
    """

    def __init__(
        self,
        catalog: SqlCatalog,
        layout: Layout,
        table: Table,
        warehouse: str | None = None,
    ) -> None:
        """Take the loaded table. `create` and `load` are what load it.

        Assigning only, so that a caller holding a `Table` from anywhere — a
        fixture, a different catalog — can build one of these around it.
        """
        self._catalog = catalog
        self._layout = layout
        # Guards the handle below and the caches keyed off it — NOT the work
        # done with them. A compaction reads files, writes Parquet and commits;
        # only the commit and the cache reads belong in here. Holding it across
        # the whole pass is what made a read wait 21.5 s behind one.
        #
        # Safe to hold so briefly because a pyiceberg table is an immutable
        # view of one snapshot: a caller that took the handle keeps working
        # against the snapshot it read, and `reload` only changes which
        # snapshot the NEXT caller sees.
        self._lock = threading.RLock()
        self._table = table
        # Where this table's files live. The local one derives it from the
        # layout; the archive is told, because its prefix is the caller's.
        self._warehouse = warehouse or layout.warehouse_uri
        # Which of the two this is, and the only thing that turns on it: the
        # ARCHIVE publishes a pointer to its own metadata, because it is the
        # one whose catalog can be lost or pointed away from. The local table's
        # catalog sits in the same directory as its warehouse — lose one and
        # you have lost the other, so a hint beside it would answer a question
        # nobody can be in a position to ask.
        self._is_archive = warehouse is not None
        # Snapshot-derived facts, cached against the metadata pointer. See
        # `extent`. The file count rides along because reading the manifests
        # produces it for free, and counting files any other way means
        # materialising every file's metadata.
        # Two caches, both keyed by the metadata pointer, because they come from
        # different depths of the metadata tree. Counts are summarised in the
        # manifest list; per-file bounds are only in the manifest entries, which
        # means opening each manifest.
        self._extent_at: str | None = None
        self._extent: tuple[int, int] | None = None
        self._counts_at: str | None = None
        # The entry walk, cached against the same pointer. Manifest MERGING
        # already collapses one manifest per commit into one for the table —
        # measured at 216 data files in a single manifest — but that is the
        # wrong axis: the cost is decoding one entry per data FILE, each
        # carrying per-column statistics, and merging cannot reduce their
        # number. Measured 14.4 ms at 216 files against 0.0 ms for the manifest
        # list, and a `maintain` pass asks three times over.
        self._files_at: str | None = None
        self._files: list[DataFile] = []
        self._file_count = 0
        self._record_count = 0

    @classmethod
    def create(
        cls, layout: Layout, schema: pa.Schema, sort_by: Sequence[str]
    ) -> LogTable:
        """Create the table and declare its sort order.

        TWO commits, not one: the catalog row lands first and the sort order
        after it. Nothing in this library reads that declaration back — `open`
        takes `sort_by` from `meta` (§4) — so it is there for anything reading
        the Iceberg table directly. `litelink.restore` cares about the split, since
        the first commit is what makes a root openable; see there.

        §4 wants the order declared as table metadata AND applied at write
        time. The declaration used to be what made `sort_by` recoverable by
        `open`; `meta` carries it now, because a failover rebuilds this table
        and has to be told what to declare.
        """
        catalog = cls._catalog_for(layout)
        catalog.create_namespace_if_not_exists(layout.table_id.split(".")[0])
        catalog.create_table(
            layout.table_id,
            schema=schema,
            location=layout.table_location,
            properties=METADATA_PROPERTIES,
        )

        table = cls(catalog, layout, catalog.load_table(layout.table_id))
        table.set_sort_order(sort_by)

        return table

    @classmethod
    def load(cls, layout: Layout, *, readonly: bool) -> LogTable:
        """Load an existing table. Raises if there is none."""
        catalog = cls._catalog_for(layout)
        table = cls(catalog, layout, catalog.load_table(layout.table_id))
        if not readonly:
            table.ensure_metadata_properties()

        return table

    @staticmethod
    def _catalog_for(layout: Layout) -> SqlCatalog:
        return SqlCatalog(
            LOCAL_CATALOG, uri=layout.catalog_uri, warehouse=layout.warehouse_uri
        )

    @classmethod
    def open_archive(
        cls,
        layout: Layout,
        prefix: str,
        options: S3Options,
        schema: pa.Schema,
        sort_by: Sequence[str] = (),
        *,
        repair: bool = False,
    ) -> LogTable:
        """The remote table, created on first use (§5).

        Its catalog is a SQLite file beside the local one and its warehouse is
        the object-store prefix — §2's two-catalog shape. Created lazily rather
        than at `litelink.new`, because a log may be configured with an archive long
        before anything is pushed to it and creating a remote table costs a
        round trip a local-only run should never pay.

        The schema is the local table's, so the two cannot drift: one declared
        shape, and the archive is the same rows later.

        The catalog entry is keyed by table id, not by warehouse, so an entry
        made for a DIFFERENT prefix would be found here and hand back a table
        whose metadata still lives in the old bucket. So it is checked against
        the prefix asked for, and replaced when it does not match.

        Checked HERE rather than dropped when the archive is re-pointed, which
        is what this did first. Re-pointing is three durable writes — the URI,
        the watermark, the catalog entry — and a crash between any two leaves
        them disagreeing; no ordering avoids it, because the damage differs in
        each direction. A check at open is a repair that runs every time, so a
        half-finished re-point corrects itself.

        It is also what keeps detach-and-reattach working. Dropping the entry
        eagerly meant pointing back at an archive that still held data built a
        fresh empty table over it, and rows already evicted locally were then
        reachable from nowhere.

        Adopting an archive that holds data but has no entry here IS
        supported, and is what makes a re-point reversible. The archive names
        its own current metadata in `version-hint.text` beside it, written at
        every commit, so a prefix with no catalog row is registered from that
        rather than created empty over the top of it.

        **Only a repairing caller adopts.** `register_table` is a write to
        `archive.db`, and a reader promised to make none — so a reader still
        gets `ArchiveAbsent` here and sees the archive once a maintenance pass
        has adopted it. Same rule as the drop above, for the same reason.
        """
        # Asked BEFORE the catalog is constructed, because constructing one
        # creates its tables in `archive.db` and registering the namespace adds
        # a row — writes, from a path that promised to make none. A reader
        # against a never-synced archive should touch nothing at all.
        boundary = prefix.rstrip("/") + "/"
        if not repair:
            try:
                known = _recorded_location(layout)
            except LookupError:
                # Could not tell offline. Fall through and let pyiceberg
                # answer, accepting the local writes that costs.
                known = ""

            if known is None:
                msg = f"no archive table at {prefix!r} yet"
                raise ArchiveAbsent(msg)

        catalog = SqlCatalog(
            ARCHIVE_CATALOG,
            uri=layout.archive_catalog_uri,
            warehouse=prefix,
            **options.resolved().catalog_properties(),
        )
        catalog.create_namespace_if_not_exists(layout.table_id.split(".")[0])

        # Read OFFLINE, before deciding anything. Whether the entry belongs to
        # this prefix is answerable from the local catalog row, and asking it
        # that way is what separates "this names another archive" from "this
        # names ours and object storage is having a bad minute".
        #
        # On a separator, not a bare prefix: `s3://b/one` is a prefix of
        # `s3://b/one-more` as a string, so a plain `startswith` accepts a
        # SIBLING archive's entry as this one's, and the log then reads and
        # writes into the neighbour it was pointed away from.
        try:
            recorded = _recorded_location(layout)
        except LookupError:
            # Could not tell. Let pyiceberg answer: it either loads the table
            # or says there is none. Slower than the offline read, and it must
            # still answer the SAME question — returning the loaded table here
            # skipped the prefix check entirely, so a process that hit a locked
            # catalog adopted the archive it had been pointed away from and
            # read, pushed and reconciled its watermark from it for the rest of
            # its life.
            try:
                recorded = catalog.load_table(layout.table_id).metadata_location
            except NoSuchTableError:
                recorded = None

        # The entry this repair displaces, kept so a failed create can put it
        # back. See below.
        displaced: str | None = None
        if recorded is not None and not recorded.startswith(boundary):
            # Another archive's table, found by table id. No read of the old
            # bucket is needed to know this, which matters when the archive
            # being left has already been taken away.
            if not repair:
                # Only a caller holding the maintenance lease may fix it.
                # Dropping and recreating is a mutation of shared state, and it
                # was reachable from any `include_archive` read — two processes
                # cold-opening after a re-point would both find the mismatch,
                # and the second's drop could land after the first had already
                # created, uploaded and committed, taking the live entry with
                # it. A reader that cannot fix it must not pretend it can.
                msg = (
                    f"the archive catalog names {recorded!r}, which is not "
                    f"under {prefix!r} — a maintenance pass repairs this"
                )
                raise ValueError(msg)

            # The entry goes; the objects do not, because detaching an
            # archive is not deleting one. Held onto, though: the create below
            # can fail — the new prefix may not exist yet, which this design
            # explicitly allows — and a half-done move that leaves NEITHER
            # entry is worse than not moving.
            #
            # It is no longer the only record of where the previous archive's
            # metadata is. It was, and that is what made a roll-back build an
            # empty table over unreachable data; `version-hint.text` in the
            # bucket is that record now, and it survives this drop because it
            # is not here.
            catalog.drop_table(layout.table_id)
            displaced, recorded = recorded, None

        if recorded is None:
            if not repair:
                # Absent, not wrong. A log configured with an archive that
                # nothing has pushed to yet is an ordinary state, and a reader
                # asking for `include_archive` before the first sync should get
                # a union without that leg — not an error, and not a table
                # created as a side effect of reading.
                msg = f"no archive table at {prefix!r} yet"
                raise ArchiveAbsent(msg)

            # ADOPT BEFORE CREATING. This is what makes re-attaching to an
            # archive expressible, which `open_archive` used to say plainly it
            # was not: "Adopting an archive that holds data but has no entry
            # here is a different operation and is NOT supported: this creates
            # an empty table at the prefix rather than discovering what is
            # already there."
            #
            # Discovering it is what `version-hint.text` is for. No listing is
            # involved — that is one GET of a known key, not the paginated walk
            # of the prefix this design refuses everywhere else.
            published = _published_location(
                load_file_io(options.resolved().catalog_properties(), prefix),
                layout,
                prefix,
            )
            try:
                if published is None:
                    table = catalog.create_table(
                        layout.table_id,
                        schema=schema,
                        location=layout.archive_table_location(prefix),
                        properties=METADATA_PROPERTIES,
                    )
                else:
                    # Deliberately unguarded, like the load below. A hint
                    # naming metadata that cannot be read is a broken archive,
                    # and the fallback available here — create an empty table —
                    # is the single worst response to it: it writes over data
                    # that is still there, having just been told where it is.
                    table = catalog.register_table(layout.table_id, published)
            except Exception:
                if displaced is not None:
                    # Put the old entry back. The repair is meant to move the
                    # log from one archive to another, and a half-done move
                    # that leaves NEITHER is the one outcome worse than not
                    # moving at all.
                    catalog.register_table(layout.table_id, displaced)

                raise

            # AFTER the rollback window, not inside it. These two are commits
            # against a table that now exists, so a failure here cannot be
            # repaired by re-registering the displaced entry — the row for this
            # table id is already taken, and `register_table` would raise
            # `TableAlreadyExistsError` from inside the handler, masking the
            # real error and skipping the rollback the comment promises.
            if published is None:
                opened = cls(catalog, layout, table, prefix)
                # DECLARED, like the local table's (§4). The archive is the
                # same rows later and is clustered the same way, so a table
                # that does not say so is lying about itself to every reader
                # that is not this library — and `sort_by` was therefore
                # unanswerable from the archive alone.
                if sort_by:
                    opened.set_sort_order(sort_by)

                # So a freshly created archive names its own metadata from the
                # start, rather than only from its first commit.
                opened.publish_pointer()
                table = opened._table  # the declaration's commit reloaded it
        else:
            # Deliberately unguarded. Catching everything here and rebuilding
            # meant a 503, a timeout or an expired token read as "there is no
            # table" — and the repair then dropped the only pointer to a live
            # archive and wrote an empty table over it, while the watermark
            # still promised eviction that those rows were safe. A failed read
            # of OUR OWN metadata is an error, not an absence.
            table = catalog.load_table(layout.table_id)

        return cls(catalog, layout, table, prefix)

    @staticmethod
    def forget(layout: Layout) -> None:
        """Drop this log's local catalog row, leaving the root unopenable.

        `create` is two commits and only the first makes a root openable, so a
        failure in the second needs undoing — see `litelink.restore`. The objects it
        may have written stay: a metadata JSON nothing references is inert, and
        the alternative is deleting files on a path already handling a failure.
        """
        LogTable._catalog_for(layout).drop_table(layout.table_id)

    @staticmethod
    def exists_for(layout: Layout) -> bool:
        """Whether the LOCAL catalog holds a table for this log.

        Not whether `catalog.db` is there. It is per-stream since 0.2, so its
        presence does say this log exists — but its ABSENCE has two meanings,
        and one of them is a live log that has not been migrated, whose catalog
        is still at the root. That case raises rather than answering False, for
        the reason spelled out below.

        Read straight out of the catalog's own SQLite, like
        `_recorded_location` and for the same reason — the question has to be
        answerable without loading the table, whose metadata may not exist yet.
        **Raises `LookupError` when it cannot tell**, which is the same refusal
        `_recorded_location` makes and for a sharper reason. `catalog.db` runs
        in `journal_mode=delete` with no busy timeout on this connection, so a
        read landing in another process's commit window returns `SQLITE_BUSY`.
        Answering False there tells `litelink.restore` it is resuming an interrupted
        restore when it is looking at a LIVE log — and the resume path then
        reserves 2**20 offsets on it, deletes every `extent` row including
        queued cuts, wipes `sealing` and `claim`, drops the archive catalog
        row, and deletes buffered rows below the frontier.
        """
        if layout.is_legacy():
            # A LIVE pre-0.2 log. False here is the destructive answer: it
            # tells `litelink.restore` it is resuming an interrupted restore,
            # and the resume path then burns 2**20 offsets on a log that is
            # still being written to, empties its buffer, and writes a fresh
            # empty catalog beside the real one — measured at 300 readable rows
            # down to 240, with eight Parquet files stranded and referenced by
            # nothing. Raising routes every caller to the same refusal a busy
            # catalog gets, which is the safe direction.
            msg = (
                f"the log at {layout.root}/{layout.name} uses the pre-0.2 "
                f"layout, whose catalogs sit at the root. Move it with:\n"
                f"  python -m litelink.migrate --root {layout.root} "
                f"--name {layout.name}"
            )
            raise LookupError(msg)

        if not layout.catalog_db.exists():
            return False

        namespace, _, name = layout.table_id.rpartition(".")
        connection = sqlite3.connect(f"file:{layout.catalog_db}?mode=ro", uri=True)
        try:
            row = connection.execute(
                "SELECT 1 FROM iceberg_tables"
                " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?",
                (LOCAL_CATALOG, namespace, name),
            ).fetchone()
        except sqlite3.Error as exc:
            msg = f"cannot read the local catalog's own table: {exc}"
            raise LookupError(msg) from exc
        finally:
            connection.close()

        return row is not None

    def exists(self) -> bool:
        return True

    # -- shape --------------------------------------------------------------

    def arrow_schema(self) -> pa.Schema:
        """The table's schema as pyarrow, `offset` included.

        Not byte-identical to what was passed to `create`: Iceberg has one
        string type, so a pyarrow `string` comes back as `large_string`. The
        logical schema is the same, and both map to the same SQLite affinity
        and the same DuckDB cast, but an equality assertion against the
        original will fail.
        """
        return schema_to_pyarrow(self._table.schema())

    def sort_by(self) -> tuple[str, ...]:
        """The declared sort order, as column names (§4)."""
        names = {f.field_id: f.name for f in self._table.schema().fields}

        return tuple(names[f.source_id] for f in self._table.sort_order().fields)

    def set_sort_order(self, sort_by: Sequence[str]) -> None:
        """Declare the sort order. Does NOT reorder existing data.

        An EMPTY order is a real value meaning unsorted, not a no-op. It used
        to return early here, so `set_sort_by((), rewrite=True)` re-clustered
        every file and left the table still declaring the old key — a table
        lying about its own clustering, with nothing able to correct it now
        that `meta` rather than this declaration is what `open` reads.
        """
        self._commit(lambda: self._apply_sort_order(sort_by))

    def add_column(self, schema: pa.Schema) -> None:
        """Widen this table to `schema`, which must be a SUPERSET of its own.

        `union_by_name` rather than `update_schema().add_column`, and the
        difference is the whole crash story. Recovery replays this step by
        probing the table, and a probe cannot be made atomic with the commit
        it checks — so the replay must be safe to run against a table where it
        has ALREADY landed. `union_by_name` is a no-op there; `add_column`
        raises `Cannot add column, name already exists` and, because
        `recover()` is unguarded in `open`, would leave the log unopenable by
        every writer while a read-only handle kept working.

        It stays strict where it matters: a column whose type genuinely
        conflicts is refused with `ValidationError: Cannot change column
        type`, verified against a live table.
        """
        self._commit(lambda: self._apply_union(schema))

    def _apply_union(self, schema: pa.Schema) -> None:
        with self._table.update_schema() as update:
            update.union_by_name(schema)

    def _apply_sort_order(self, sort_by: Sequence[str]) -> None:
        with self._table.update_sort_order() as update:
            for column in sort_by:
                update.asc(column, IdentityTransform())

    # -- state ------------------------------------------------------------

    def reload(self) -> None:
        """Point at the current snapshot, never at an older one.

        The load happens INSIDE the lock, which looks like holding a mutex
        across I/O for no reason and is not. Loading outside it and assigning
        after let two concurrent reloads land in completion order rather than
        snapshot order: a slow load of an older snapshot finishing last
        installs it, and the handle goes backwards.

        A reader then straddles the regression. `_query` resolves a floor from
        the newer snapshot, reads the buffer tail above it, and resolves again
        — landing on the older one. Its table leg scans the older snapshot
        while its buffer leg holds only rows above the newer boundary, so
        everything in between is in neither: rows silently missing from the
        answer, which is the failure this whole read path is arranged to make
        impossible.

        A catalog resolve is ~0.5 ms and reads already serialise on the
        reader's own lock, so ordering costs nothing worth having.
        """
        with self._lock:
            self._table = self._catalog.load_table(self._layout.table_id)

    @property
    def metadata_location(self) -> str:
        return str(self._table.metadata_location)

    @property
    def properties(self) -> dict[str, str]:
        return dict(self._table.properties)

    def is_empty(self) -> bool:
        return self._table.current_snapshot() is None

    def snapshot_count(self) -> int:
        """How many snapshots the table still carries — expiry's visible effect."""
        return int(self._table.inspect.snapshots().num_rows)

    def offset_field_id(self) -> int:
        return self._table.schema().find_field("litelink_offset").field_id

    # -- reads from statistics --------------------------------------------

    def data_files(self) -> list[DataFile]:
        """Current data files with their offset extents, ordered by offset.

        Extents come from manifest column statistics, so no data file is
        opened — which is what makes the tier boundary cheap enough for §7 to
        derive it on every read.
        """
        with self._lock:
            location = self.metadata_location
            if location == self._files_at:
                return self._files

            self._files = self._read_files()
            self._files_at = location

            return self._files

    def _read_files(self) -> list[DataFile]:
        """The entry walk itself. Cached by `data_files`."""
        if self.is_empty():
            return []

        files = self._table.inspect.files()
        field = self._table.schema().find_field("litelink_offset")
        found = [
            DataFile(
                path=_local(path),
                size=int(size),
                rows=int(rows),
                lo=from_bytes(field.field_type, dict(lower)[field.field_id]),
                hi=from_bytes(field.field_type, dict(upper)[field.field_id]),
            )
            for path, size, rows, lower, upper in zip(
                files["file_path"].to_pylist(),
                files["file_size_in_bytes"].to_pylist(),
                files["record_count"].to_pylist(),
                files["lower_bounds"].to_pylist(),
                files["upper_bounds"].to_pylist(),
                strict=True,
            )
        ]

        return sorted(found, key=lambda f: f.lo)

    def extent(self) -> tuple[int, int] | None:
        """`(lo, hi)` over the current snapshot — §7's tier boundary.

        Cached against `metadata_location`, which is the version pointer: an
        unchanged pointer is the same snapshot, so the extent cannot have moved.
        This does not weaken §7's "resolve per query, never pin" — the resolve
        still happens, at ~0.5 ms, and is what decides whether the cache stands.

        The caching is not an optimisation so much as a correction. §7 describes
        this read as ~0.6 ms and "fixed rather than proportional", but reading
        it from manifests costs time proportional to FILE COUNT — measured at
        1.0 ms for one file and 46 ms for 64. Between commits that work is
        entirely redundant, and on a read-heavy log it was nearly all of the
        per-query overhead.
        """
        with self._lock:
            self._refresh()

            return self._extent

    def snapshot(self) -> tuple[str, tuple[int, int] | None]:
        """The metadata pointer and the extent it belongs to, read together.

        Two calls could not be paired safely: a commit between them hands a
        reader a new snapshot with an old boundary, or the reverse, and each
        tear corrupts the union differently — one duplicates the rows in the
        gap, the other loses them.
        """
        with self._lock:
            self._refresh()

            return self.metadata_location, self._extent

    def file_count(self) -> int:
        """How many data files the current snapshot holds.

        From the manifest list, which summarises each manifest — so this reads
        one file rather than opening every manifest. Measured at 60 files:
        0.65 ms against 42.9 ms for the entry walk.
        """
        with self._lock:
            self._refresh_counts()

            return self._file_count

    def record_count(self) -> int:
        """How many rows the current snapshot holds.

        Also summarised in the manifest list. Counting by scanning instead reads
        the offset column out of every Parquet file — correct, and proportional
        to the data, which is the wrong shape for anything polling.
        """
        with self._lock:
            self._refresh_counts()

            return self._record_count

    def _refresh_counts(self) -> None:
        location = self.metadata_location
        if location == self._counts_at:
            return

        snapshot = self._table.current_snapshot()
        files = records = 0
        if snapshot is not None:
            for manifest in snapshot.manifests(self._table.io):
                # Live entries are ADDED plus EXISTING; DELETED are tombstones
                # a compaction or eviction left behind.
                files += (manifest.added_files_count or 0) + (
                    manifest.existing_files_count or 0
                )
                records += (manifest.added_rows_count or 0) + (
                    manifest.existing_rows_count or 0
                )

        self._file_count, self._record_count = files, records
        self._counts_at = location

    def _refresh(self) -> None:
        location = self.metadata_location
        if location != self._extent_at:
            self._extent = self._read_extent()
            self._extent_at = location

    def _read_extent(self) -> tuple[int, int] | None:
        """Offset bounds straight off the manifest entries.

        Deliberately not `inspect.files()`, which materialises an 18-column
        Arrow table — including `readable_metrics`, which decodes the bounds of
        every column — to answer a question about one. Measured at roughly half
        the cost across file counts, and it still opens no data file.
        """
        snapshot = self._table.current_snapshot()
        if snapshot is None:
            return None

        field = self._table.schema().find_field("litelink_offset")
        lows: list[int] = []
        highs: list[int] = []
        for manifest in snapshot.manifests(self._table.io):
            for entry in manifest.fetch_manifest_entry(
                self._table.io, discard_deleted=True
            ):
                lows.append(
                    from_bytes(
                        field.field_type, entry.data_file.lower_bounds[field.field_id]
                    )
                )
                highs.append(
                    from_bytes(
                        field.field_type, entry.data_file.upper_bounds[field.field_id]
                    )
                )

        return None if not lows else (min(lows), max(highs))

    def file_paths(self) -> set[str]:
        return {f.path for f in self.data_files()}

    def referenced_paths(self) -> set[str]:
        """Every file any live snapshot needs — data and Iceberg's own."""
        return {
            _local(path)
            for path in self._table.inspect.all_files()["file_path"].to_pylist()
        } | self.metadata_paths(self._table.snapshots())

    def metadata_paths(self, snapshots: Iterable[Snapshot]) -> set[str]:
        """Manifest lists and manifests belonging to `snapshots`.

        Iceberg's own bookkeeping, which `expire_snapshots` does not delete —
        verified against pyiceberg 0.11.1: expiring four of five snapshots left
        all ten avro files on disk. Two per commit accumulates fast on a stream
        that seals every few minutes.
        """
        paths: set[str] = set()
        for snapshot in snapshots:
            paths.add(_local(snapshot.manifest_list))
            paths.update(
                _local(manifest.manifest_path)
                for manifest in snapshot.manifests(self._table.io)
            )

        return paths

    def snapshots_older_than(self, cutoff: datetime) -> list[Snapshot]:
        """Snapshots eligible for expiry, excluding the current one."""
        current = self._table.current_snapshot()
        current_id = None if current is None else current.snapshot_id

        return [
            snapshot
            for snapshot in self._table.snapshots()
            if snapshot.timestamp_ms / 1000 < cutoff.timestamp()
            and snapshot.snapshot_id != current_id
        ]

    def snapshot_ages(self) -> dict[str, datetime]:
        """File path -> when the snapshot that added it committed.

        §2 stamps no ingest column, so a file's age is its snapshot's commit
        time and nothing else.
        """
        snapshots = self._table.inspect.snapshots()
        committed = dict(
            zip(
                snapshots["snapshot_id"].to_pylist(),
                snapshots["committed_at"].to_pylist(),
                strict=True,
            )
        )

        entries = self._table.inspect.entries()
        ages: dict[str, datetime] = {}
        for snapshot_id, data_file in zip(
            entries["snapshot_id"].to_pylist(),
            entries["data_file"].to_pylist(),
            strict=True,
        ):
            added = committed.get(snapshot_id)
            if added is not None:
                ages[_local(data_file["file_path"])] = added

        return ages

    # -- commits ------------------------------------------------------------
    #
    # Iceberg commits are optimistic: the commit asserts the branch has not
    # moved, and pyiceberg raises rather than retrying. §11 already says what
    # should happen instead — "the loser refreshes and retries" — so that lives
    # here rather than in each caller.
    #
    # This is what makes a second process possible. A manager process running
    # maintenance beside a capturing writer loses the race whenever a seal
    # commits mid-compaction; without the retry it dies on
    # CommitFailedException, which is exactly what it did before this existed.

    def _commit(self, operation: Callable[[], None]) -> None:
        """Run a commit, refreshing and retrying if the branch moved under it.

        Safe to replay because a failed commit landed nothing: the range a
        compaction rewrites sits below the boundary a concurrent seal appends
        above, so the second attempt selects the same files as the first.
        """
        for attempt in range(_COMMIT_ATTEMPTS):
            try:
                with self._lock:
                    operation()
            except CommitFailedException:
                if attempt == _COMMIT_ATTEMPTS - 1:
                    raise

                # Backed off, with jitter, before the retry. Five immediate
                # attempts against a contended branch is a thundering herd:
                # every loser reloads and re-commits at once, so they collide
                # again, and the exception that escapes is not a failure of the
                # work but of the timing. Reachable in ordinary operation now
                # that passes claim ranges rather than a role — two maintainers
                # on disjoint offsets is the point of that, and it means two of
                # them committing to one Iceberg branch. Range-disjointness
                # makes their DATA independent; it does nothing about the
                # single branch pointer they both swap.
                time.sleep(random.uniform(0, _COMMIT_BACKOFF_MS * (2**attempt)) / 1000)
                self.reload()
                # Refreshed, so check it is still the same table. The catalog
                # row is keyed by table id, not by identity, and a `set_archive`
                # racing a slow register replaces what that row names — so the
                # reload silently re-binds this operation to the NEW archive
                # and the retry commits paths that live in the old bucket. The
                # new archive's manifests would then reference objects the
                # re-point retired, and the next sync's reconcile would launder
                # that extent into the watermark eviction acts on.
                self._verify_identity()
            else:
                self.reload()
                # After the reload, so it names the metadata this commit
                # actually produced rather than the one it was built from.
                self.publish_pointer()

                return

    def _verify_identity(self) -> None:
        """Refuse to keep operating on a table that left this warehouse.

        The one durable write in a push with no watermark fence around it is
        the Iceberg commit itself, and `_commit`'s retry is where it can change
        which table it means.
        """
        location = str(self._table.metadata_location)
        boundary = self._warehouse.rstrip("/") + "/"
        if not location.startswith(boundary):
            msg = (
                f"the table moved out of {self._warehouse!r} while this commit "
                f"was in flight (now {location!r}); it was not retried"
            )
            raise RuntimeError(msg)

    def uri(self, rel_path: str) -> str:
        """Where a root-relative file lives in this table's warehouse."""
        return f"{self._warehouse.rstrip('/')}/{rel_path}"

    def put(self, source: Path, rel_path: str) -> None:
        """Upload a local file into this table's warehouse (§5 step 1).

        Through pyiceberg's own FileIO rather than a second S3 client, so the
        credentials and endpoint that reach the archive are the ones the
        catalog was built with — one place to configure, and no way for an
        upload to land somewhere the table cannot then read.

        Overwrites. The name carries a per-attempt token, so a repeat is the
        same sync replaying the same file, and finishing it is what makes the
        pass restartable.
        """
        destination = self.uri(rel_path)
        with source.open("rb") as reading:
            payload = reading.read()

        output = self._table.io.new_output(destination)
        with output.create(overwrite=True) as writing:
            writing.write(payload)

    def publish_pointer(self) -> None:
        """Record which metadata JSON is current, beside them in the warehouse.

        Called after every archive commit, so the bucket always names its own
        current metadata. Two things need that and neither can get it from
        `archive.db`: re-attaching to an archive this log was pointed away from
        (the catalog row is gone — that is what re-pointing does), and reading
        the archive from anywhere that is not this machine.

        **Best effort, and it has to be.** The commit has already landed; the
        table is correct whether or not this succeeds. Raising here would turn
        a published archive into a failed sync and send the caller into a retry
        of work that is done. A hint that fails to write is a hint that still
        names the previous metadata — behind, never wrong, and corrected by the
        next commit.

        Written AFTER the commit, never as part of it. A hint published from
        inside an attempt would name metadata that a CAS retry then superseded,
        pointing readers at a snapshot the table moved off.

        Published after `_commit`'s reload rather than before it, though that
        turns out not to be load-bearing: pyiceberg updates the handle in place
        when a commit lands, so `metadata_location` already names the winner
        before the reload re-reads it from the catalog. Measured — publishing
        first produces the same hint. Kept in this order because reading the
        pointer at the point the table is known-current needs no argument.
        """
        if not self._is_archive:
            return

        path, version = _hint_for(str(self._table.metadata_location))
        try:
            with self._table.io.new_output(path).create(overwrite=True) as writing:
                writing.write(version.encode())
        except Exception:
            # Deliberately swallowed, and deliberately not logged from a
            # library. The next commit rewrites it; a sync that raised here
            # would be reporting a failure that did not happen.
            return

    def fetch(self, path: str, destination: Path) -> None:
        """Download a file out of this table's warehouse. Inverse of `put`.

        Through the catalog's own FileIO for the same reason `put` is: the
        credentials that reach the archive are the ones the table was opened
        with, so there is no second client to configure and no way to read from
        somewhere the table does not point.

        Whole-file, not streamed. These are `target_compact_size` files, the
        same amount compaction holds in memory to write one, and the caller is
        an explicit operation rather than anything on a read path.
        """
        payload = self._table.io.new_input(path).open().read()
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Written under a temporary name and renamed, so a crash cannot leave a
        # short file under a name the table is about to reference. Rename is
        # atomic within a directory; the fsync is what makes the bytes precede
        # it (§2).
        staged = destination.with_name(f"{destination.name}.partial")
        staged.write_bytes(payload)
        fsync(staged)
        staged.replace(destination)

    def remove(self, path: str) -> None:
        """Delete a file from this table's warehouse.

        Through the catalog's FileIO, like `put` and `fetch`, so one set of
        credentials reaches object storage and a delete cannot be aimed
        somewhere the table does not point.
        """
        self._table.io.delete(path)

    def key(self, path: str) -> str:
        """The warehouse-relative name of a file in this table.

        The inverse of `uri`, and what lets an archived file be placed locally
        under the same name it has remotely — so hydrating twice writes the
        same path rather than accumulating copies.
        """
        return path.removeprefix(self._warehouse.rstrip("/") + "/")

    def register(
        self,
        paths: list[str],
        sealed_through: int | None = None,
        archived_through: int = 0,
        lo: int | None = None,
    ) -> bool:
        """Add already-written files to the table, in ONE commit (§4 step 2).

        `add_files` rather than `append`: pyiceberg's append writes the file
        itself and commits afterwards, so a crash in between orphans a file
        under a name nothing recorded — exactly what I2 exists to prevent.

        `sealed_through` is the exclusive end of the range this file covers,
        and passing it makes the commit a no-op if the range is already in the
        table. That is what closes the race two owners can otherwise win.

        Iceberg already serialises them: both compare-and-swap against the same
        pointer, one moves it, the other raises `CommitFailedException`. What
        broke it was OUR retry — reloading and trying again, which succeeds,
        because per-attempt names mean the second file does not collide with
        the first. The CAS was doing its job and we were overriding it.

        Re-checked on every attempt rather than once, because `_commit` reloads
        between them: the loser's retry now sees the winner's file covering its
        range and does nothing. Both orderings are safe — a writer that reloads
        after the winner never attempts at all.

        Takes a `list`, deliberately, not a `Sequence`: a `str` satisfies
        `Sequence[str]`, so a single path passed unwrapped type-checks and then
        registers the file's CHARACTERS. That is not hypothetical — it is what
        happened when this signature changed.

        **Several paths per commit, because a commit is the expensive part.**
        Measured against S3: 648 ms to upload a file and 4.1 s to register it,
        because each commit reads the new file's footer, writes a manifest, a
        manifest list and a fresh metadata.json, and rewrites the catalog
        pointer — none of which gets cheaper for holding one file instead of
        twenty. One commit per file made `sync` take 83 s over sixteen files
        and starved the sealer that shared its thread.


        Returns whether the file was added. False means the range was already
        covered, so this file is redundant — and the caller has to queue it for
        deletion, or it is a file on disk that nothing records.
        """
        added = True

        def add() -> None:
            nonlocal added
            if sealed_through is not None and (
                self._covers(sealed_through) or archived_through >= sealed_through - 1
            ):
                added = False

                return

            self._refuse_straddle(lo)
            added = True
            self._table.add_files(paths)

        self._commit(add)

        return added

    def _refuse_straddle(self, lo: int | None) -> None:
        """Refuse a range that PARTIALLY overlaps what this table holds.

        The last line of defence, and the only one that cannot be reasoned
        around. `_covers` declines a range entirely covered, which makes a
        replayed push harmless — but a range that starts inside the extent and
        ends beyond it is admitted, and those rows are then in two files at
        once, in the immutable tier, with nothing able to repair it.

        Everything upstream is arranged so this cannot arise: compaction skips
        files the archive holds, so a merge never straddles its extent. That
        argument has a gap, and it is narrow enough to have survived several
        reviews — a crash between a register and the row recording it, then a
        compaction-config change before the next sync backfills, regroups
        pushed-but-unrecorded files into a mergeable run. The upstream fix for
        each such path is a fresh piece of reasoning; this is one check that
        holds however the reasoning turns out.

        The test is `lo <= covered[1]`, with no lower bound, and the missing
        lower bound is the point. `covered[0] <= lo` was there first and was a
        hole rather than a safety condition: it exempted exactly the range that
        starts BELOW the extent and spans past it, engulfing the whole thing —
        every archived offset in two files at once, which is the worst version
        of what this exists to stop, not an excused one.

        Nothing legitimate is refused. `sync` pushes only files above the
        archive's extent, so a batch whose first file starts at or below
        `covered[1]` necessarily contains that offset — the last row of the
        archive's top file, a real archived row — and is a genuine overlap
        however it is shaped. Whole-batch replays are excused earlier, by
        `_covers`.

        Refusing costs a stall, and the stall is worse than this used to say.
        The straddling file never lands, the watermark stops, eviction pins
        below it, and **nothing re-cuts a local straddler**: `rewrite_archive`
        works the other side, and no tool does this one. The refusal is still
        right — a loud permanent stall beats a silent permanent duplication —
        but calling it recoverable was wrong, and the operator's only route
        today is to lower the compaction target so the straddler is left alone,
        or to start a fresh archive prefix.

        Reaching it at all takes a crash between a register and the rows
        recording it, and then a compaction-target change before the next sync
        backfills those rows from the archive's manifest. SPEC §4a records the
        window and what would close it.
        """
        if lo is None:
            return

        covered = self.extent()
        if covered is not None and lo <= covered[1]:
            msg = (
                f"refusing a range starting at {lo}, which reaches into this "
                f"table's extent {covered} without being covered by it: "
                "admitting it would put those offsets in two files at once"
            )
            raise ValueError(msg)

    def _covers(self, sealed_through: int) -> bool:
        """Whether the table already holds everything below `sealed_through`.

        Data files cover contiguous, non-overlapping offset ranges (§4), so the
        extent's upper bound answers this on its own.

        An EMPTY table answers False, which is why `register` also consults the
        archive watermark. A writer stalled between renewing its lease and
        registering, while another owner sealed the same range, archived it and
        evicted the table to nothing, would otherwise resume and re-add its
        stale file — and a local table holding only [100, 199] under an archive
        holding [0, 999] serves 200-999 from no leg at all, until eviction
        drops it again up to `local_retention` later.
        """
        extent = self.extent()

        return extent is not None and extent[1] >= sealed_through - 1

    def replace_range(self, lo: int, hi: int, paths: Sequence[str]) -> None:
        """Swap `[lo, hi]` for already-written files, in one snapshot (§6).

        `overwrite()` would do this in a single call, but it writes the output
        itself — putting a path on disk this process only learns about
        afterwards, which is what the deletion queue exists to avoid.

        Several paths because an archive rewrite re-cuts a range into however
        many correctly sized files it takes, and the swap has to be one
        snapshot: committing them one at a time would mean each commit deleting
        a sub-range of a file the next commit still needs.
        """

        def swap() -> None:
            with self._table.transaction() as transaction:
                transaction.delete(delete_filter=offset_between(lo, hi))
                transaction.add_files(list(paths))

        self._commit(swap)

    def evict_through(self, boundary: int) -> None:
        """Drop every file at or below `boundary` from the current snapshot (§8)."""
        self._commit(
            lambda: self._table.delete(delete_filter=offset_at_or_below(boundary))
        )

    def expire_snapshots_older_than(self, cutoff: datetime) -> None:
        """Expire snapshot METADATA. Does not delete any file — see §6."""
        self._commit(
            lambda: (
                self._table.maintenance.expire_snapshots().older_than(cutoff).commit()
            )
        )

    def scan_range(self, lo: int, hi: int) -> pa.Table:
        return self._table.scan(row_filter=offset_between(lo, hi)).to_arrow()

    def ensure_metadata_properties(self) -> None:
        """Apply the metadata-retention properties to a table that predates them."""
        missing = {
            key: value
            for key, value in METADATA_PROPERTIES.items()
            if self._table.properties.get(key) != value
        }
        if not missing:
            return

        self._commit(lambda: self._set_properties(missing))

    def _set_properties(self, properties: dict[str, str]) -> None:
        with self._table.transaction() as transaction:
            transaction.set_properties(properties)


def _local(path: object) -> str:
    """Iceberg records `file://` URIs; the filesystem wants plain paths."""
    return str(path).removeprefix("file://")
