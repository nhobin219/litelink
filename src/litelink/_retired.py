"""Truncating and deleting a retired log, from its published table alone (#181).

A retired log is only its published table. `retire` drains the buffer, evicts
the whole staging table and checks nothing is left local, so even where the
log's directory survives there is nothing local to truncate — and where it does
not, as after a failover, the published location is all there is. So both
operations here take that location and a name, not a handle: a retired log
cannot be opened for writing, and a read-only handle stays read-only.

**Truncation never deletes a log.** `delete` is the caller's decision, taken
once whatever names the log (for streamcast, a stream's `metadata.json`) has
stopped naming it.
"""

from __future__ import annotations

import contextlib
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from pyarrow.fs import FileSelector, FileType
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import StaticTable

from litelink._buffer import Buffer
from litelink._config import DEFAULT_PUBLISHED_SNAPSHOT_RETENTION
from litelink._layout import Layout
from litelink._s3 import S3Options
from litelink._table import (
    PUBLISHED_CATALOG,
    RETIRED_PROPERTY,
    VERSION_HINT,
    LogTable,
    _Catalog,
    _published_location,
    catalog_name,
    published_retired,
    shared_file_io,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from os import PathLike

# Concurrent deletes, as the sweep and the published drain run them: one
# object-store delete is a round trip.
DELETE_THREADS = 32


def truncate(
    published: str,
    name: str,
    *,
    below: int,
    s3_options: S3Options | None = None,
    published_snapshot_retention: timedelta = DEFAULT_PUBLISHED_SNAPSHOT_RETENTION,
) -> int:
    """Drop a RETIRED log's history below offset `below`, from its published
    table alone, and return the floor reached (#181).

    The live log's `WriteHandle.truncate`, for a log that can no longer be
    opened for writing — and that may have no local directory at all. Its
    rules are the same: whole files only, so the floor snaps down to the start
    of a file straddling `below`; `below` is exclusive; nothing past what the
    published table holds, which for a retired log is everything; and the
    offset counter is untouched. **Not reversible.** A log that is not
    retired is refused: its truncation needs its local claims and tier rows.

    **Each call first deletes what earlier calls left due**, since no
    maintainer runs on a retired log. The live log's deletion queue is in its
    local SQLite, which this cannot rely on, so what is due is derived from
    the table's snapshot history instead:

    1. kept: every snapshot younger than `published_snapshot_retention`, and
       the one that was current at the cutoff;
    2. a data file only older snapshots reference stopped being referenced
       before the cutoff — timed from the commit that removed it, not from the
       snapshot that last held it — so its grace has passed, and it is
       deleted;
    3. then those snapshots expire, and the metadata nothing references any
       more is swept.

    Deleted BEFORE the expiry, so a crash between the two leaves the files
    still named and the next call finds them again; the other order orphans
    them. The files this call truncates go on a later call, once a retention
    has passed — the grace a reader already scanning them needs.

    **One caller at a time per log.** It commits through a throwaway catalog
    adopted from the published table's `version-hint.text`, so two callers
    would each commit onto what the hint said when they started. Each commit
    first checks the hint still names the metadata it started from, and
    raises `RuntimeError` if not; calling again then starts from the current
    one.
    """
    if below < 0:
        msg = f"below must be an offset, not {below}"
        raise ValueError(msg)

    options = s3_options or S3Options()
    properties = options.resolved().catalog_properties()
    with tempfile.TemporaryDirectory(prefix="litelink-truncate-") as scratch:
        layout = Layout(Path(scratch), name)
        layout.create()
        hint = shared_file_io(properties, published)
        location = _published_location(hint, layout, published)
        if location is None:
            msg = f"no published table for {name!r} under {published!r}"
            raise FileNotFoundError(msg)

        # Retired, asked of the very metadata adopted below — read once, so a
        # `delete` landing in between cannot make them disagree.
        if (
            RETIRED_PROPERTY
            not in StaticTable.from_metadata(location, properties).properties
        ):
            msg = (
                f"{name!r} under {published!r} is not a retired log. Truncate a "
                f"live log through its handle: `log.truncate(below=...)`"
            )
            raise ValueError(msg)

        # ADOPTED, never created: registered at exactly the metadata the hint
        # named. `open_published` would create an empty table if the hint had
        # gone in the meantime — a concurrent `delete` — and that table, with
        # no `litelink.retired`, would refuse every later `delete`.
        catalog = _Catalog(
            catalog_name(layout.published_db, PUBLISHED_CATALOG),
            uri=layout.published_catalog_uri,
            warehouse=published,
            **properties,
        )
        catalog.create_namespace_if_not_exists(layout.table_id.split(".")[0])
        table = LogTable(
            catalog,
            layout,
            catalog.register_table(layout.table_id, location),
            published,
        )

        def unmoved() -> None:
            current = _published_location(hint, layout, published)
            if current != table.metadata_location:
                msg = (
                    f"the published table of {name!r} moved while this truncate ran "
                    f"(now {current!r}) — another call is truncating it. Retry once "
                    f"that one finishes"
                )
                raise RuntimeError(msg)

        boundary = f"{layout.published_table_location(published).rstrip('/')}/"
        _reclaim(table, published_snapshot_retention, boundary, unmoved)

        table.reload()
        files = table.data_files()
        floor = below
        # Down to a file boundary: each move lands on some file's start.
        moved = True
        while moved:
            moved = False
            for data_file in files:
                if data_file.start < floor < data_file.end:
                    floor = data_file.start
                    moved = True

        if any(f.end <= floor for f in files):
            unmoved()
            table.evict_below(floor)

        return floor


def delete(
    published: str,
    name: str,
    *,
    s3_options: S3Options | None = None,
    root: PathLike[str] | str | None = None,
) -> None:
    """Delete a RETIRED log entirely: everything under `<published>/<name>/` —
    its data, its metadata and its WAL replica — and, given `root`, its local
    directory there (#181).

    What a log's directory holds is litelink's layout, so the caller names
    the root rather than unlinking files it would have to know about; and
    unlike a caller's own `rmtree`, this checks first that the `buffer.db`
    there says the log is retired. A litestream sidecar still running for the
    log should be stopped first: it has nothing left to replicate, and loses
    its database.

    **Immediate, with no grace.** litelink cannot know who still reads the
    log, so the caller first removes it from whatever names it, and waits out
    its readers. Truncation never calls this.

    A log that is not retired is refused — by the published table's
    `litelink.retired`, and by the local `buffer.db` when `root` holds one.
    **Resumable:** the files that say the log is retired go last — every other
    object, then every superseded `metadata.json`, then `version-hint.text`,
    then the `metadata.json` it named — and the local directory after them,
    so a crash at any point leaves enough to say so, and calling again
    finishes. Listed as a directory, not a string prefix, so `trades` never
    matches `trades-v2`.
    """
    options = s3_options or S3Options()
    local = None if root is None else Layout(Path(root), name)
    if local is not None and local.buffer_db.exists():
        marker = Buffer.peek_retired(local.buffer_db)
        if marker is None or marker.get("state") != "retired":
            msg = (
                f"{local.directory} is not a retired log; only a retired log is deleted"
            )
            raise ValueError(msg)

    layout = local or Layout(Path(tempfile.gettempdir()), name)
    location = layout.published_table_location(published).rstrip("/")
    properties = options.resolved().catalog_properties()
    io = shared_file_io(properties, published)
    if not isinstance(io, PyArrowFileIO):
        msg = f"cannot list {published!r}: its FileIO is not pyarrow's"
        raise TypeError(msg)

    scheme, netloc, path = PyArrowFileIO.parse_location(location, io.properties)
    fs = io.fs_by_scheme(scheme, netloc)
    listed = [
        info.path
        for info in fs.get_file_info(
            FileSelector(path, recursive=True, allow_not_found=True)
        )
        if info.type == FileType.File
    ]

    def remove(target: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            fs.delete_file(target)

    if listed:
        hint = [p for p in listed if p.endswith(f"/metadata/{VERSION_HINT}")]
        metadata = sorted(p for p in listed if p.endswith(".metadata.json"))
        if hint:
            named = _published_location(io, layout, published)
            current = (
                None
                if named is None
                else PyArrowFileIO.parse_location(named, io.properties)[2]
            )
            retired = published_retired(layout, published, options) is not None
        else:
            # A crash after the hint went: only `metadata.json` files are left,
            # and the newest — the one the hint named — still says retired.
            # Anything else here is not what this leaves, and is refused.
            current = metadata[-1] if metadata else None
            retired = (
                current is not None
                and len(metadata) == len(listed)
                and RETIRED_PROPERTY
                in StaticTable.from_metadata(
                    f"{scheme}://{current}", properties
                ).properties
            )

        if not retired:
            msg = (
                f"the published table of {name!r} under {published!r} records no "
                f"retirement; only a retired log is deleted"
            )
            raise ValueError(msg)

        last = {*hint, *metadata}
        with ThreadPoolExecutor(max_workers=DELETE_THREADS) as pool:
            list(pool.map(remove, [p for p in listed if p not in last]))
            list(pool.map(remove, [p for p in metadata if p != current]))

        for target in hint:
            remove(target)

        if current is not None:
            remove(current)

    if scheme == "file":
        # Directories are a local disk's only: object storage has none to leave.
        with contextlib.suppress(FileNotFoundError):
            fs.delete_dir(path)

    if local is not None and local.directory.exists():
        shutil.rmtree(local.directory)


def _reclaim(
    table: LogTable,
    retention: timedelta,
    boundary: str,
    unmoved: Callable[[], None],
) -> None:
    """Delete what earlier truncations left due, then expire the snapshots
    that named it and sweep their metadata (see `truncate`)."""
    table.reload()
    cutoff = datetime.now(UTC) - retention
    # The snapshot current at the cutoff stays: a file it references may have
    # stopped being referenced only since, inside the grace. Found along the
    # lineage, from the current snapshot back to the first older than the
    # cutoff — the current one itself when nothing has committed since — and
    # not by sorting timestamps, which two writers' skewed clocks can misorder.
    cutoff_ms = cutoff.timestamp() * 1000
    current_then = next(
        (s.snapshot_id for s in table.lineage() if s.timestamp_ms < cutoff_ms), None
    )
    expiring = [
        s for s in table.snapshots_older_than(cutoff) if s.snapshot_id != current_then
    ]

    if expiring:
        ids = {s.snapshot_id for s in expiring}
        kept = [s for s in table.snapshots() if s.snapshot_id not in ids]
        due = [
            path
            for path in table.data_paths(expiring) - table.data_paths(kept)
            if path.startswith(boundary)
        ]
        unmoved()
        _remove_all(table, due)
        unmoved()
        table.expire_snapshots(sorted(ids))

    # What no snapshot or `metadata.json` still names, older than the
    # retention — the expired snapshots' manifests and lists among it.
    # Listed first and read after, so a commit landing between counts as
    # live (`Maintenance._stranded`).
    table.reload()
    anchors = table.anchors()
    listed = table.metadata_files()
    table.reload()
    live = table.referenced_metadata()
    if not anchors <= {path for path, _ in listed}:
        return

    unmoved()
    _remove_all(
        table,
        [
            path
            for path, written in listed
            if written < cutoff and path not in live and path.startswith(boundary)
        ],
    )


def _remove_all(table: LogTable, paths: list[str]) -> None:
    def remove(path: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            table.remove(path)

    with ThreadPoolExecutor(max_workers=DELETE_THREADS) as pool:
        list(pool.map(remove, paths))
