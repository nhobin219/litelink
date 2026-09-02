"""Building logs and readers.

`log.py` owns what the handles *do*; this module owns how they
come to exist. The split is why `litelink.follow` could return something that was
not a `WriteHandle` and read as though it should — a classmethod names its receiver as
the thing being built, and four of these build three different types.

Every factory here builds its object's collaborators and hands them over
complete. None of them takes a mode: `open` builds a writer, `reader` and
`follow` build readers, and there is no flag that turns one into the other.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

import pyarrow as pa

from litelink._archive import ARCHIVE_KEY, Archive
from litelink._buffer import (
    CONFIG_KEY,
    SCHEMA_KEY,
    SORT_KEY,
    START_OFFSET_KEY,
    Buffer,
)
from litelink._layout import Layout, validate_archive
from litelink._maintenance import Maintenance
from litelink._read import Reader, duckdb_connection
from litelink._replication import litestream_config, restore_buffer
from litelink._table import LogTable, archive_extent
from litelink.log import (
    LocalReadHandle,
    LogConfig,
    LogHandle,
    RemoteReadHandle,
    WriteHandle,
    _declared_schema,
    application_schema,
    table_schema,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from os import PathLike

    from litelink._s3 import S3Options


@overload
def open(  # noqa: A001
    root: PathLike[str] | str,
    name: str,
    *,
    read_only: Literal[False] = False,
    s3: S3Options | None = None,
) -> WriteHandle: ...


@overload
def open(  # noqa: A001
    root: PathLike[str] | str,
    name: str,
    *,
    read_only: Literal[True],
    s3: S3Options | None = None,
) -> LocalReadHandle: ...


def open(  # noqa: A001
    root: PathLike[str] | str,
    name: str,
    *,
    read_only: bool = False,
    s3: S3Options | None = None,
) -> LogHandle:
    """Open an existing log, for writing or for reading beside its writer.

    **One constructor, two types out.** The overloads above give the precise
    class for a literal `read_only`, so `open(root, name).append(...)` checks
    and `open(root, name, read_only=True).append(...)` does not — the builtin
    `open()` is typed the same way, returning `TextIOWrapper` or
    `BufferedReader` from the mode literal. Passing a non-literal falls back to
    `LogHandle` and the caller narrows.

    That is the difference from the flag this replaced. `litelink.open(read_only=…)`
    returned ONE class whose thirteen write methods existed and raised, so
    misuse was invisible until it ran. Here read-only returns a class that has
    no write methods at all.

    Takes none of the log's shape: columns, config, archive and sort order all
    come from the log itself, so nothing at the call site can disagree with
    what is on disk.

    **Read-only recovers nothing**, which is the point of it. Finishing an
    interrupted seal is the writer's to do, and a second process doing it is a
    race — `examples/adsb/replicate.py` runs beside a live writer and became
    one for a commit, taking both whole-log claims and queueing the other
    process's Parquet for deletion. SQLite is opened `mode=ro` and the Iceberg
    table read-only, so this cannot advance the log even by accident.

    Reading sees the writer's commits as they land: `catalog.db` and
    `archive.db` live at the root and both processes read the same rows. That
    is the difference from `follow`, which reads a replica captured at a point
    in time.
    """
    layout = Layout(Path(root), name)
    table, schema = _existing(layout, name, readonly=read_only)
    buffer = Buffer.open(layout.buffer_db, schema, readonly=read_only)
    try:
        config = _validated_shape(layout, buffer, name)
        remote = Archive(layout, buffer, s3)
        reader = Reader(layout, table, buffer, duckdb_connection, archive=remote)
        if read_only:
            return LocalReadHandle(
                layout=layout,
                table=table,
                buffer=buffer,
                archive=remote,
                reader=reader,
            )

        handle = WriteHandle(
            layout=layout,
            table=table,
            buffer=buffer,
            reader=reader,
            maintenance=Maintenance(table, buffer, layout, remote),
            config=config,
            archive=remote,
        )
    except BaseException:
        buffer.close()

        raise

    handle.recover()

    return handle


def follow(
    name: str,
    *,
    archive: str,
    s3: S3Options | None = None,
    binary: str | None = None,
    scratch_dir: PathLike[str] | str | None = None,
) -> RemoteReadHandle:
    """A read-only view of a log running somewhere else (§3b).

    The archive merged with a replicated copy of the writer's buffer, so a
    reader sees data fresher than the archive alone — down to the replication
    lag rather than to the seal cadence. It never takes over as writer and
    never writes anything the primary shares.

    **This is `restore`'s assembly without the takeover.** `restore` burns
    `RESTORE_RESERVE` offsets to fence a machine that may still be writing; a
    follower appends nothing, so there is nothing to fence and it reserves
    none.

    **The result is a `RemoteReadHandle`**, a sibling of the `LocalReadHandle`
    that `open(read_only=True)` returns.
    What makes it behave as a follower is its state, not its type: the local
    Iceberg table is empty by construction and the archive is known to hold
    rows, so `LogHandle` derives that the archive is load-bearing and both
    includes it and refuses to serve without it. A local reader whose table has
    been fully evicted meets the same two conditions and gets the same
    treatment, correctly — it used to serve 476 of 1,500 rows instead.

    **A snapshot, not a subscription.** litestream restores to a point in
    time, so refreshing means assembling another one — exit the block and call
    this again. The root is always a temporary directory the reader owns and
    removes on close; `scratch_dir` chooses where it lives, because the
    restored buffer holds the unsealed tail *plus* the band the archive lacks
    and `/tmp` is often memory-backed.

    There is deliberately no `root` argument. It had no use case and carried
    two latent bugs: a caller-supplied root could land on a directory that
    already held a live log, whose `catalog.db` and `archive.db` are shared by
    every log under it, and could leave a stale `archive.db` to win over the
    bucket's own hint.

    **An archive that has published nothing is not automatically refused.**
    That is the ordinary state of a slow capture — nothing reaches
    `target_seal_size`, and with `wal_replication` a seal retains its rows — so
    the buffer holds the whole log and the WAL carries every row there is. The
    follower serves it alone when `_incomplete_buffer` can prove nothing is
    missing, and refuses naming the band when it cannot.
    """
    # First, because this path does not go through `validate` — a follower has
    # no config to validate — and a malformed prefix would otherwise surface as
    # a YAML parse error from the litestream subprocess, three frames down and
    # naming a generated file the caller never sees.
    validate_archive(archive)
    options = _options(s3)
    owned = tempfile.TemporaryDirectory(
        prefix="litelink-follow-",
        dir=None if scratch_dir is None else Path(scratch_dir),
    )
    try:
        layout = Layout(Path(owned.name), name)
        _restore_replica(layout, archive, options, binary)
        schema, sort_by, prefix = _followed_shape(layout)
        _assemble_follower(layout, schema, sort_by, prefix, options)
        table = LogTable.load(layout, readonly=True)
        buffer = Buffer.open(layout.buffer_db, schema, readonly=True)
        try:
            remote = Archive(layout, buffer, options)
            view = RemoteReadHandle(
                layout=layout,
                table=table,
                buffer=buffer,
                archive=remote,
                reader=Reader(layout, table, buffer, duckdb_connection, archive=remote),
                owned=owned,
            )
        except BaseException:
            buffer.close()

            raise
    except BaseException:
        owned.cleanup()

        raise

    return view


def _options(s3: S3Options | None) -> S3Options:
    from litelink._s3 import S3Options as _S3Options

    return s3 or _S3Options()


def _existing(
    layout: Layout, name: str, *, readonly: bool
) -> tuple[LogTable, pa.Schema]:
    """The log's table and declared schema, or a message naming what is wrong."""
    # Asked BEFORE `exists_for`, which refuses to answer for a legacy tree
    # rather than reporting one as absent — a distinction that is destructive
    # to get wrong on the restore path. Here it is only a matter of saying
    # which: told "use new()", an operator creates an empty log beside data
    # that is still there.
    if layout.is_legacy():
        msg = (
            f"the log at {layout.root}/{name} uses the pre-0.2 layout, whose "
            f"catalogs sit at the root. Move it with:\n"
            f"  python -m litelink.migrate --root {layout.root} --name {name}"
        )
        raise FileNotFoundError(msg)

    try:
        present = LogTable.exists_for(layout)
    except LookupError:
        present = True

    if not present:
        msg = f"no litelink log at {layout.root}/{name} — use new() to create one"
        raise FileNotFoundError(msg)

    table = LogTable.load(layout, readonly=readonly)

    return table, _declared_schema(layout, application_schema(table.arrow_schema()))


def _validated_shape(layout: Layout, buffer: Buffer, name: str) -> LogConfig:
    """Fail fast on a damaged log, with a message naming it.

    `new` always writes both of these, so an absent or unparseable value is a
    damaged log rather than an older one. Quietly substituting defaults would
    change how a log seals and what it retains without saying so, and would
    de-cluster every file the next compaction rewrote.
    """
    encoded = Buffer.peek_meta(layout.buffer_db, CONFIG_KEY)
    if encoded is None:
        msg = f"log at {layout.root}/{name} has no stored config; it is corrupt"
        raise ValueError(msg)

    try:
        buffer.sort_by()
    except ValueError as exc:
        msg = f"log at {layout.root}/{name} has no stored sort order; it is corrupt"
        raise ValueError(msg) from exc

    return LogConfig.from_json(encoded)


def _restore_replica(
    layout: Layout,
    archive: str,
    options: S3Options,
    binary: str | None,
) -> None:
    """Pull `buffer.db` down, with the sidecar's config kept out of the root.

    `restore` writes that config into the root at its START, because
    `restore_buffer` needs `-config` to run. A follower must not leave one
    there: `litestream_config` keys each replica on the path RELATIVE to the
    root, so a follower with the same log name produces a key identical to the
    primary's. Running a sidecar in that directory — which this project's own
    convention says to do — would ship the follower's stripped scratch copy
    over the primary's only off-box record of its unsealed rows.

    So it goes in a temporary directory of its own and is deleted with it, on
    the exception path too. Nothing needs it once the restore returns.

    This used to begin by refusing a root that already held a log, because
    `follow` took a `root` argument and a caller could aim it anywhere. The
    argument is gone and the root is always freshly made, so the check could no
    longer fire; it was removed rather than left as a guard that reads like
    protection and provides none.
    """
    layout.root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="litelink-follow-cfg-") as cfg:
        config_path = Path(cfg) / "litestream.yml"
        config_path.write_text(litestream_config(layout, archive, options))
        restore_buffer(config_path, layout.buffer_db, options, binary)

    if not layout.buffer_db.exists():
        # Both readings, because nothing in the arguments separates them.
        msg = (
            f"no replica of {layout.buffer_db.name} under {archive} — there is "
            f"nothing to follow. A log with wal_replication off has no off-box "
            f"copy of its unsealed rows, or `name` and `archive` do not "
            f"describe a log that exists"
        )
        raise FileNotFoundError(msg)


def _followed_shape(layout: Layout) -> tuple[pa.Schema, tuple[str, ...], str]:
    """The log's shape, and where its archive is, from the replica's `meta`.

    The prefix comes from here rather than from `follow`'s `archive=`, which
    only says where the WAL replica lives. That is also why the archive
    pre-flight cannot come first: there is no prefix to check until `buffer.db`
    is on disk.
    """
    encoded = Buffer.peek_meta(layout.buffer_db, CONFIG_KEY)
    raw_schema = Buffer.peek_meta(layout.buffer_db, SCHEMA_KEY)
    raw_sort = Buffer.peek_meta(layout.buffer_db, SORT_KEY)
    if encoded is None or raw_schema is None or raw_sort is None:
        msg = (
            f"the replica of {layout.root}/{layout.name} carries no stored shape; "
            f"it is corrupt or was captured mid-creation"
        )
        raise ValueError(msg)

    prefix = Buffer.peek_meta(layout.buffer_db, ARCHIVE_KEY)
    if not prefix:
        msg = (
            f"{layout.root}/{layout.name} records no archive, so there is nothing "
            f"to merge the replicated buffer with. A local-only log cannot be "
            f"followed — its sealed files exist only on the machine that wrote them"
        )
        raise ValueError(msg)

    return (
        pa.ipc.read_schema(pa.py_buffer(bytes.fromhex(raw_schema))),
        tuple(json.loads(raw_sort)),
        prefix,
    )


def _incomplete_buffer(layout: Layout, schema: pa.Schema) -> str | None:
    """Why the restored buffer is not the whole log, or None if it is.

    Asked only when the archive publishes no pointer, where the buffer is the
    only tier that can serve. Returns a phrase for the caller's message so the
    ways of falling short are named rather than collapsed into one number.

    **Rows leave the buffer two ways and arrive outside it a third, and each
    needs its own evidence.** This took three attempts and a review round each,
    so the shape is worth stating:

    - `finish_seal(discard=True)` and `release_archived` delete a PREFIX, so
      they raise the buffer's first offset above the log's. The log's own first
      offset is `start_offset` in `meta`, absent meaning 1 (§2).
    - `ingest` never puts its rows in the buffer at all, so it raises nothing
      and leaves a hole strictly INSIDE the buffered range. `INGESTED_KEY`
      records it, written at reservation time so a crash cannot under-report.

    An earlier version keyed the second on `extent` rows naming local files.
    That is a copy of what the Iceberg manifest owns and the code tolerates it
    being absent, so a crash between the register and the record, or an ordinary
    compaction unioning a loaded range with held rows on either side, left the
    check permissive. `orphaned_local_ranges` survives only as a FALLBACK for
    logs written before the key existed, where it can add refusals but never
    remove one.

    An empty buffer is complete only if the log never issued anything, which
    `next_offset` answers from `sqlite_sequence` rather than from rows a seal
    may have taken.
    """
    probe = Buffer.open(layout.buffer_db, schema, readonly=True)
    try:
        recorded = probe.get_meta(START_OFFSET_KEY)
        first_issued = int(recorded) if recorded else 1
        held = probe.extent()
        first_held = probe.next_offset() if held is None else held[0]
        if first_held != first_issued:
            return f"it starts at offset {first_held} rather than {first_issued}"

        loaded = probe.ingested_through()
        if loaded:
            return (
                f"offsets up to {loaded} were bulk-loaded straight to Parquet "
                f"and never entered it"
            )

        orphaned = probe.orphaned_local_ranges()
        if orphaned:
            lo, hi = orphaned[0]
            more = f" (and {len(orphaned) - 1} more)" if len(orphaned) > 1 else ""

            return f"offsets {lo}..{hi - 1} are in a local file it lacks{more}"

        return None
    finally:
        probe.close()


def _assemble_follower(
    layout: Layout,
    schema: pa.Schema,
    sort_by: tuple[str, ...],
    prefix: str,
    options: S3Options,
) -> None:
    """Adopt the archive and build the empty local table a reader expects.

    **Neither `repair` value adopts safely on its own, which is why the bucket
    is asked first.** `repair=False` never creates — and never adopts either:
    it reads the local `archive.db` row, which a follower has none of, and
    `Archive.table` swallows the resulting `ArchiveAbsent` into `None`. The
    reader would then serve the buffer alone, silently missing every archived
    row. `repair=True` adopts, but with no published hint it takes the CREATE
    branch and writes a `metadata.json` and a `version-hint.text` into the
    bucket — a reader publishing a lineage the primary then commits onto.

    `archive_extent` reads the hint from the bucket alone, with no `archive.db`
    and no catalog, and separates "no hint" from "cannot read". Once it has
    answered, `repair=True` cannot reach the create branch, because a published
    hint is exactly what it proved.

    The local table is created EMPTY and stays that way, which is what makes
    `LogHandle` treat the archive as load-bearing without being told to.
    """
    import contextlib

    covered = archive_extent(layout, prefix, options)
    if covered is None:
        # Nothing published, so the buffer is the only tier that can serve —
        # allowed exactly when it holds the whole log. That is the case a slow
        # capture lives in: WAL replication carries every row it has, and until
        # something seals and syncs there is nothing else to carry.
        shortfall = _incomplete_buffer(layout, schema)
        if shortfall is not None:
            msg = (
                f"{prefix!r} publishes no metadata pointer, so the replicated "
                f"buffer is the only tier — and {shortfall}. Those rows are in "
                f"neither tier, and following this would serve a short log "
                f"with no error"
            )
            raise ValueError(msg)

    buffer = Buffer.open(layout.buffer_db, schema)
    try:
        # Reserves nothing: a follower never appends, so there is no offset to
        # fence. `strip_local_state` still runs — the `extent` rows naming the
        # primary's Parquet describe files this machine cannot open, and the
        # copy is scratch, so dropping them is free.
        buffer.strip_local_state(0)
        # Adoption is SKIPPED when nothing is published, not attempted.
        # `repair=True` against a prefix with no hint takes the CREATE branch —
        # a reader writing a `metadata.json` and a `version-hint.text` into the
        # bucket, onto which the primary would then commit. Asking the bucket
        # first is what guards that, and it is unchanged; what changed is that
        # "empty" no longer has to mean "refuse".
        #
        # `_archive_required` then answers False for this follower — empty local
        # table, archive resolving to nothing — so reads come from the buffer
        # alone, which `_incomplete_buffer` has just established is the log.
        adopted = None
        extent = None
        if covered is not None:
            remote = Archive(layout, buffer, options)
            if remote.table(repair=True) is None:
                msg = (
                    f"restored the buffer but could not adopt the archive at "
                    f"{prefix!r}: it holds nothing this log can read"
                )
                raise RuntimeError(msg)

            adopted = remote.table()
            extent = None if adopted is None else adopted.extent()

        if extent is not None:
            buffer.release_archived(extent[1])
            buffer.reseed_group()
            # The replica's own watermark is whatever the primary had recorded
            # when litestream last shipped, which can be 0 — a buffer captured
            # before the first sync. The BUCKET is authoritative and was just
            # read, so record what the archive actually holds.
            #
            # Not bookkeeping: `LogHandle._archive_required` derives "the
            # archive is the only source of the archived rows" from this
            # watermark and an empty local table, and a stale 0 makes a
            # follower decide it needs no archive — then serve nothing at all,
            # because `release_archived` above just emptied its buffer.
            buffer.set_meta_all({Maintenance.ARCHIVED_KEY: str(extent[1])})
    finally:
        buffer.close()

    try:
        LogTable.create(layout, table_schema(schema), sort_by)
    except Exception:
        with contextlib.suppress(Exception):
            LogTable.forget(layout)

        raise


def new(
    root: PathLike[str] | str,
    name: str,
    *,
    schema: pa.Schema,
    sort_by: Sequence[str] | None = None,
    config: LogConfig | None = None,
    archive: str | None = None,
    s3: S3Options | None = None,
    start_offset: int = 1,
) -> WriteHandle:
    """Create a log. See `litelink.new` for the shape it fixes and why."""
    return WriteHandle.new(
        root,
        name,
        schema=schema,
        sort_by=sort_by,
        config=config,
        archive=archive,
        s3=s3,
        start_offset=start_offset,
    )


def restore(
    root: PathLike[str] | str,
    name: str,
    *,
    archive: str,
    s3: S3Options | None = None,
    binary: str | None = None,
) -> WriteHandle:
    """Take over a log whose machine is gone, fencing the offsets it may have
    assigned. See `litelink.restore`."""
    return WriteHandle.restore(root, name, archive=archive, s3=s3, binary=binary)


__all__ = ["follow", "new", "open", "restore"]
