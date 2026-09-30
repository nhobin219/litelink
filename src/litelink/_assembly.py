"""Building logs and readers.

`log.py` owns what the handles *do*; this module owns how they come to exist.

Every factory here builds its object's collaborators and hands them over
complete. `open` builds a writer or, with `read_only=True`, a reader on the same
host; `new` and `restore` build writers. Every handle reads on the primary —
reading a log from another machine is any Iceberg engine over its archive, not
something litelink assembles (#90).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

import pyarrow as pa

from litelink._archive import Archive
from litelink._buffer import (
    CONFIG_KEY,
    Buffer,
)
from litelink._layout import Layout
from litelink._maintenance import Maintenance
from litelink._read import Reader, duckdb_connection
from litelink._table import LogTable
from litelink.log import (
    LocalReadHandle,
    LogConfig,
    LogHandle,
    WriteHandle,
    _declared_schema,
    application_schema,
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
    `archive.db` live at the root and both processes read the same rows.
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
    handle._backfill_archive_bounds()  # noqa: SLF001

    return handle


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
    return WriteHandle.restore(
        root,
        name,
        archive=archive,
        s3=s3,
        binary=binary,
    )


__all__ = ["new", "open", "restore"]
