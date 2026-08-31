"""Move a log from the pre-0.2 layout to the per-stream one (SPEC §2).

    python -m litelink.migrate --root ./data --name trades          # dry run
    python -m litelink.migrate --root ./data --name trades --apply

**What moves, and what deliberately does not.** Data files do not move: they
were always written to `<root>/<name>/data`, which is where they belong under
the new layout too. What moves is everything that was somewhere else —

    <root>/catalog.db            ->  <root>/<name>/catalog.db
    <root>/archive.db            ->  <root>/<name>/archive.db
    <root>/litelink/<name>/metadata  ->  <root>/<name>/metadata

and the same metadata move inside the archive prefix. Not a rename: Iceberg
metadata carries absolute self-references, so `*.metadata.json` has its
`location`, `manifest-list` and `metadata-log` paths rewritten, and each
manifest LIST is re-encoded with new `manifest_path` entries. Manifests
themselves are copied byte for byte, because the only paths in them are data
file paths and those are the ones that stay put.

**Why not simply recreate the table at the new location.** `add_files` against
the existing Parquet would be a fraction of this code and it would be wrong.
Retention derives a file's age from the commit time of the snapshot that added
it — `LogTable.snapshot_ages`, because §2 stamps no ingest column — so a fresh
commit stamps every file with the migration's own timestamp and resets the
retention clock on both tiers at once. Preserving snapshot ids and timestamps
is the whole reason this rewrites pointers instead.

**The WAL replica is not moved, and that is deliberate.** Its objects are LTX
segments belonging to a chain litestream owns, keyed by a replica path that has
changed — and `catalog.db` no longer holds the same bytes, since a shared
catalog is split per stream here. Relocating that chain would produce a replica
that restores a database nobody wrote. Instead: migrate, restart the sidecar so
it establishes a fresh replica under `<prefix>/<name>/_wal`, confirm it landed,
and only then drop the old `<prefix>/_wal` with `--drop-legacy-wal`.

**A root holding several streams is migrated one stream at a time, and three
things are shared until the last of them has moved**: `catalog.db`,
`archive.db` and the root `litestream.yml`, which names every stream's buffer.
They are kept while any stream still resolves through them, and the old sidecar
should be left running — an unmigrated stream's layout has not changed, and
that config still describes it correctly.

`<prefix>/_wal` is shared the same way, and `--drop-legacy-wal` is run once per
stream. It refuses while any stream in the root is outstanding, refuses in the
same pass as the migration, and refuses until something has actually been
replicated to `<prefix>/<name>/_wal` — three ways of saying that the old
replica holds the only off-box copy of unsealed rows, which are by definition
in no Parquet file and no archive manifest. When it does run it deletes the
keys it can NAME — this stream's buffer replica, the buffers of any streams the
shared catalog still lists, and the two shared catalogs — never the prefix.
Nothing binds an archive prefix to one root, and 0.1's own advice for several
logs was a root each, so a wholesale delete would reach another root's replicas.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from litelink._layout import NAMESPACE, Layout
from litelink._replication import WAL_PREFIX, destination
from litelink._s3 import S3Options
from litelink._table import ARCHIVE_CATALOG, LOCAL_CATALOG

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pyiceberg.io import FileIO


@dataclass(frozen=True, slots=True)
class Step:
    """One thing the migration will do, printable before it does it."""

    action: str
    detail: str

    def __str__(self) -> str:
        return f"  {self.action:<9} {self.detail}"


@dataclass
class Plan:
    """Everything the migration would do, and what it refuses to do."""

    root: Path
    name: str
    archive: str | None = None
    steps: list[Step] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def needed(self) -> bool:
        return bool(self.steps)

    def __str__(self) -> str:
        lines = [f"litelink layout migration: {self.root}/{self.name}"]
        if self.archive:
            lines.append(f"archive: {self.archive}")

        lines.append("")
        if self.blockers:
            lines.append("REFUSING:")
            lines += [f"  - {b}" for b in self.blockers]
            lines.append("")

        if not self.steps:
            lines.append("  nothing to do — already on the per-stream layout")
        else:
            lines += [str(s) for s in self.steps]

        if self.notes:
            lines.append("")
            lines += [f"note: {n}" for n in self.notes]

        return "\n".join(lines)


def is_legacy(root: Path, name: str) -> bool:
    """Whether this log still keeps its catalogs at the root.

    Keyed on `catalog.db`, which every log has, rather than on the metadata
    directory, which a log that has never sealed does not.
    """
    return Layout(root, name).is_legacy()


def _legacy_metadata_dir(root: Path, name: str) -> Path:
    return root / NAMESPACE / name / "metadata"


def _rewrite_paths(value: Any, old: str, new: str) -> Any:
    """Replace an `old` location prefix with `new`, anywhere in a JSON tree.

    Blanket rather than field-by-field on purpose: it catches `location`,
    every snapshot's `manifest-list` and every `metadata-log` entry without
    this having to enumerate a metadata schema that Iceberg is free to extend.

    It cannot touch a data file path, which is the property that makes a
    blanket replace safe here: data lives at `<root>/<name>/data` and the old
    location was `<root>/litelink/<name>`, so no data path has the old prefix.
    """
    if isinstance(value, str):
        return new + value[len(old) :] if value.startswith(old) else value

    if isinstance(value, list):
        return [_rewrite_paths(item, old, new) for item in value]

    if isinstance(value, dict):
        return {key: _rewrite_paths(item, old, new) for key, item in value.items()}

    return value


def _manifest_lists(documents: Sequence[dict[str, Any]]) -> set[str]:
    """Every manifest-list path named by any of these metadata documents.

    Read from the metadata rather than inferred from the `snap-` filename
    prefix. The prefix is pyiceberg's convention, not a format guarantee, and
    the cost of guessing wrong is a manifest list copied verbatim with stale
    `manifest_path` entries inside it — an archive that reads short, silently.
    """
    return {
        snapshot["manifest-list"]
        for document in documents
        for snapshot in document.get("snapshots", [])
        if snapshot.get("manifest-list")
    }


def _rewrite_manifest_list(
    io: FileIO,
    source: str,
    target: str,
    snapshot: dict[str, Any],
    old: str,
    new: str,
) -> None:
    """Re-encode one manifest list with its `manifest_path` entries moved."""
    from pyiceberg.manifest import read_manifest_list, write_manifest_list

    entries = list(read_manifest_list(io.new_input(source)))
    for entry in entries:
        path = entry.manifest_path
        if path.startswith(old):
            # By position, because `manifest_path` is a read-only property on
            # pyiceberg's Record. Position 0 is `manifest_path` in every
            # manifest-list schema Iceberg has published, v1 and v2 alike.
            entry[0] = new + path[len(old) :]

    with write_manifest_list(
        format_version=2,
        output_file=io.new_output(target),
        snapshot_id=snapshot["snapshot-id"],
        parent_snapshot_id=snapshot.get("parent-snapshot-id"),
        sequence_number=snapshot.get("sequence-number"),
        avro_compression="deflate",
    ) as writer:
        writer.add_manifests(entries)


def _migrate_metadata(
    io: FileIO,
    entries: Sequence[str],
    old_dir: str,
    new_dir: str,
    old_location: str,
    new_location: str,
) -> None:
    """Copy a metadata directory to a new location, rewriting what points.

    Order matters only in that nothing is deleted here: every write lands at
    the new location and the old one is untouched, so a failure half way
    through leaves the log exactly as it was and the migration can be re-run.
    """
    documents: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if entry.endswith(".metadata.json"):
            documents[entry] = json.loads(io.new_input(entry).open().read())

    lists = _manifest_lists(list(documents.values()))
    snapshots = {
        snapshot["manifest-list"]: snapshot
        for document in documents.values()
        for snapshot in document.get("snapshots", [])
        if snapshot.get("manifest-list")
    }

    for entry in entries:
        target = f"{new_dir}/{entry.rsplit('/', 1)[1]}"
        if entry in documents:
            rewritten = _rewrite_paths(documents[entry], old_location, new_location)
            with io.new_output(target).create(overwrite=True) as handle:
                handle.write(json.dumps(rewritten).encode())
        elif entry in lists:
            _rewrite_manifest_list(
                io, entry, target, snapshots[entry], old_location, new_location
            )
        else:
            # Manifests and `version-hint.text`, byte for byte. A manifest's
            # only paths are data file paths, which do not move; the hint holds
            # a bare metadata stem with no directory in it at all.
            data = io.new_input(entry).open().read()
            with io.new_output(target).create(overwrite=True) as handle:
                handle.write(data)


def _split_catalog(
    source: Path, target: Path, catalog: str, name: str, rewrite: tuple[str, str] | None
) -> None:
    """Copy a shared catalog to one stream's directory, keeping its row only.

    A copy rather than a move, because the source may still be answering for
    sibling streams that have not been migrated yet. Pruning the other rows is
    what makes the copy a per-stream catalog rather than N identical ones: a
    row naming a table whose metadata is not under this directory is a row that
    can only mislead.

    **Through SQLite's backup API, never `copyfile`.** These databases run in
    WAL mode, so the most recent commits — including, on a live log, the very
    `metadata_location` this migration exists to move — sit in `catalog.db-wal`
    and not in `catalog.db`. A file copy takes the main database alone and
    silently rewinds the catalog to its last checkpoint. `backup` reads through
    a connection and so sees the WAL, and the checkpoint at the end leaves the
    target self-contained rather than depending on a sidecar file of its own.
    """
    # Built beside the destination and MOVED into place, never written to it
    # directly. `is_legacy` answers on the existence of `<name>/catalog.db`, so
    # a crash part way through a direct write leaves a truncated database that
    # nonetheless makes the log look migrated — and every re-run then prints
    # "nothing to do" over a tree that cannot be opened. `os.replace` is atomic
    # within a filesystem, so the file appears whole or not at all.
    staged = target.with_name(target.name + ".partial")
    staged.unlink(missing_ok=True)

    origin = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        copy = sqlite3.connect(staged)
        try:
            origin.backup(copy)
            copy.execute(
                "DELETE FROM iceberg_tables"
                " WHERE NOT (catalog_name = ? AND table_name = ?)",
                (catalog, name),
            )
            if rewrite is not None:
                old, new = rewrite
                for column in ("metadata_location", "previous_metadata_location"):
                    copy.execute(
                        f"UPDATE iceberg_tables SET {column} = ? || substr({column}, ?)"  # noqa: S608
                        f" WHERE {column} LIKE ? || '%'",
                        (new, len(old) + 1, old),
                    )

            copy.commit()
            # TRUNCATE, so what lands beside the log is one file with every
            # commit in it. Left unchecked, the deletes above would live in a
            # `-wal` that nothing here is responsible for.
            copy.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            copy.close()
    finally:
        origin.close()

    # The checkpoint above leaves the staged database self-contained, so there
    # are no sidecar files to carry across with it.
    for suffix in ("-wal", "-shm"):
        Path(str(staged) + suffix).unlink(missing_ok=True)

    os.replace(staged, target)


def _s3_filesystem(options: S3Options) -> Any:
    """A pyarrow filesystem for the archive, from the same options as the log.

    pyarrow rather than s3fs: `pyiceberg[pyarrow]` is already a runtime
    dependency and s3fs is not, so a migration that needed it would fail on
    exactly the installs that have an archive to migrate.
    """
    from pyarrow.fs import S3FileSystem

    resolved = options.resolved()
    named = {
        "access_key": resolved.access_key,
        "secret_key": resolved.secret_key,
        "region": resolved.region,
        "endpoint_override": resolved.endpoint,
    }

    return S3FileSystem(**{k: v for k, v in named.items() if v is not None})


def _entries(directory: str, options: S3Options | None) -> list[str]:
    """Every file directly in `directory`, as full URIs. Empty if absent.

    The one place this library lists anything, and it is worth naming why that
    is allowed here: every other path refuses a listing because it would be a
    paginated walk standing in for state SQLite already holds. A migration has
    no such record — it is moving files the catalog names only one of — and it
    runs once, by hand.
    """
    if directory.startswith("s3://"):
        from pyarrow.fs import FileSelector

        if options is None:
            return []

        filesystem = _s3_filesystem(options)
        selector = FileSelector(directory.removeprefix("s3://"), allow_not_found=True)

        return [
            f"s3://{info.path}"
            for info in filesystem.get_file_info(selector)
            if info.is_file
        ]

    local = Path(directory.removeprefix("file://"))
    if not local.is_dir():
        return []

    return [f"file://{path}" for path in sorted(local.iterdir()) if path.is_file()]


def _sibling_streams(catalog: Path, name: str) -> list[str]:
    """Other streams that still RESOLVE through a shared catalog.

    Rows alone are not the question. The shared file keeps every stream's row
    for ever — this migration never deletes from it, so that a half-finished
    run leaves the untouched streams working — and counting rows would mean the
    last stream to migrate still saw its already-migrated siblings and left the
    shared catalog behind for nobody.

    So: a sibling counts only while it has no catalog of its own.
    """
    if not catalog.exists():
        return []

    connection = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT DISTINCT table_name FROM iceberg_tables WHERE table_name != ?",
            (name,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        connection.close()

    root = catalog.parent

    return [row[0] for row in rows if not (root / row[0] / "catalog.db").exists()]


def build_plan(
    root: Path,
    name: str,
    *,
    archive: str | None = None,
    s3: S3Options | None = None,
    drop_legacy_wal: bool = False,
) -> Plan:
    """What migrating this log would do. Reads; changes nothing."""
    root = Path(root).resolve()
    layout = Layout(root, name)
    plan = Plan(root=root, name=name, archive=archive)

    if not layout.buffer_db.exists():
        plan.blockers.append(
            f"no log named {name!r} under {root} — buffer.db is absent"
        )

        return plan

    if archive and s3 is None:
        # Refused rather than skipped. The archive half would be left out while
        # `archive.db`'s pointer was still rewritten to the new prefix, so the
        # catalog would name a location nothing had been copied to.
        plan.blockers.append(
            "an archive was named but no S3 options were given, so its metadata "
            "cannot be moved. Pass s3=, or omit archive= to migrate only the "
            "local tier"
        )

        return plan

    if drop_legacy_wal and archive:
        legacy_wal = f"{archive.rstrip('/')}/{WAL_PREFIX}"
        # Shared per root, exactly as `catalog.db` is: it holds `catalog.db`,
        # `archive.db` AND every stream's `buffer.db` replica, keyed by the
        # path relative to the ROOT. A sibling that has not migrated still has
        # its only off-box copy of unsealed rows in there — rows that are by
        # definition in no Parquet file and no archive manifest — so dropping
        # it early is unrecoverable loss, not untidiness.
        fresh = destination(archive, name)
        remaining = _sibling_streams(root / "catalog.db", name)
        if remaining:
            plan.blockers.append(
                f"refusing --drop-legacy-wal: {', '.join(sorted(remaining))} "
                f"still replicate through {legacy_wal}, which holds the only "
                f"off-box copy of their unsealed rows. Migrate them first, then "
                f"drop it. Nothing binds an archive prefix to one root, so check "
                f"that no other root replicates there either"
            )
        elif is_legacy(root, name):
            # A SECOND pass, always. Dropping in the same run as the migration
            # leaves a window with no replica at all: the old one is gone and
            # the sidecar has not written a new one yet, and the rows at risk
            # in that window are exactly the unsealed ones no Parquet file
            # holds.
            plan.blockers.append(
                "refusing --drop-legacy-wal in the same pass as the migration. "
                "Migrate first, restart the sidecar so it writes to "
                f"{fresh}, confirm that landed, then re-run with the flag"
            )
        elif not _prefix_holds(fresh, s3, only_files=True):
            plan.blockers.append(
                f"refusing --drop-legacy-wal: nothing has been replicated to "
                f"{fresh} yet, so {legacy_wal} is still the only off-box copy "
                f"of this log's unsealed rows. Restart the sidecar with the "
                f"config at <root>/<name>/litestream.yml and re-run. If this "
                f"log no longer replicates at all, remove the old prefix with "
                f"your own object-store tooling instead"
            )
            # `only_files` above, and it is the difference between testing what
            # this says and testing something weaker. Object stores leave
            # zero-byte `.../` keys behind, pyarrow reports them as
            # directories, and counting them would let a marker holding no data
            # answer "something has been replicated here" — unlocking the
            # strongest of these three gates on a replica that does not exist.
        elif _prefix_holds(legacy_wal, s3):
            owned = _legacy_wal_keys(root, name, legacy_wal)
            plan.steps.append(
                Step("drop-wal", f"remove {len(owned)} owned key(s) under {legacy_wal}")
            )
        else:
            plan.notes.append(f"nothing under {legacy_wal} — already dropped")

    if not is_legacy(root, name):
        return plan

    legacy_catalog = root / "catalog.db"
    legacy_archive = root / "archive.db"
    legacy_metadata = _legacy_metadata_dir(root, name)

    plan.steps.append(Step("catalog", f"{legacy_catalog} -> {layout.catalog_db}"))
    if legacy_archive.exists():
        plan.steps.append(Step("catalog", f"{legacy_archive} -> {layout.archive_db}"))

    local_entries = _entries(f"file://{legacy_metadata}", None)
    if local_entries:
        plan.steps.append(
            Step(
                "metadata",
                f"{legacy_metadata} -> {layout.directory / 'metadata'}"
                f" ({len(local_entries)} files)",
            )
        )

    if archive:
        old = f"{archive.rstrip('/')}/{NAMESPACE}/{name}/metadata"
        remote_entries = _entries(old, s3)
        if remote_entries:
            plan.steps.append(
                Step(
                    "archive",
                    f"{old} -> {layout.archive_table_location(archive)}/metadata"
                    f" ({len(remote_entries)} objects)",
                )
            )
        else:
            plan.notes.append(
                f"no archive metadata found at {old} — nothing to move remotely"
            )

    # Only when this is the last stream. The shared config is kept while any
    # sibling still resolves through it, so promising its removal here would
    # contradict the note the apply run prints.
    if (root / "litestream.yml").exists() and not _sibling_streams(
        root / "catalog.db", name
    ):
        plan.steps.append(
            Step(
                "config", f"remove stale {root / 'litestream.yml'} (replica path moved)"
            )
        )

    siblings = _sibling_streams(legacy_catalog, name)
    if siblings:
        plan.notes.append(
            f"{legacy_catalog.name} also holds {', '.join(sorted(siblings))}; "
            f"the shared copy stays until every stream is migrated"
        )

    plan.notes.append(
        "data files are not touched — they were already at <root>/<name>/data"
    )
    if archive:
        plan.notes.append(
            "the WAL replica is NOT moved: restart the sidecar so it writes to "
            f"{archive.rstrip('/')}/{name}/_wal, confirm it landed, then re-run "
            "with --drop-legacy-wal"
        )

    return plan


def _records(io: FileIO, metadata_uri: str) -> int | None:
    """Rows in the current snapshot, straight from the metadata document.

    The migration's own check that it moved a table rather than broke one.
    Read from the summary rather than counted from the data, because the
    summary is what every reader trusts and a mismatch there is the failure
    worth catching.
    """
    document = json.loads(io.new_input(metadata_uri).open().read())
    current = document.get("current-snapshot-id")
    for snapshot in document.get("snapshots", []):
        if snapshot["snapshot-id"] == current:
            total = snapshot.get("summary", {}).get("total-records")

            return None if total is None else int(total)

    return None


def _pointer(catalog: Path, catalog_name: str, name: str) -> str | None:
    """The metadata location one catalog records for one table."""
    if not catalog.exists():
        return None

    connection = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)
    try:
        row = connection.execute(
            "SELECT metadata_location FROM iceberg_tables"
            " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?",
            (catalog_name, NAMESPACE, name),
        ).fetchone()
    except sqlite3.Error:
        return None
    finally:
        connection.close()

    return None if row is None else row[0]


def migrate(
    root: Path,
    name: str,
    *,
    archive: str | None = None,
    s3: S3Options | None = None,
    drop_legacy_wal: bool = False,
) -> Plan:
    """Do it. Writes everything new before removing anything old."""
    from pyiceberg.io import load_file_io

    root = Path(root).resolve()
    layout = Layout(root, name)
    plan = build_plan(
        root, name, archive=archive, s3=s3, drop_legacy_wal=drop_legacy_wal
    )
    if plan.blockers:
        return plan

    # On `is_legacy`, NOT on `plan.needed`. The plan also carries the
    # drop-the-old-WAL step, which is a SECOND pass by design — run after the
    # sidecar has established a fresh replica — so a plan can have steps while
    # the layout is already migrated. Branching on `needed` sent that second
    # pass back through the whole migration on a tree that had already moved.
    if not is_legacy(root, name):
        _drop_legacy_wal(plan, root, name, archive, s3, drop=drop_legacy_wal)

        return plan

    legacy_catalog = root / "catalog.db"
    legacy_archive = root / "archive.db"
    legacy_metadata = _legacy_metadata_dir(root, name)

    local_io = load_file_io({}, str(layout.directory))
    local_old = f"file://{root / NAMESPACE / name}"
    before = _pointer(legacy_catalog, LOCAL_CATALOG, name)
    expected = None if before is None else _records(local_io, before)

    # Metadata first, catalogs second. The catalog row is the pointer; writing
    # it before the files it names would leave a window where the log resolves
    # to metadata that is not there yet, and a crash inside that window is a
    # log that will not open.
    local_entries = _entries(f"file://{legacy_metadata}", None)
    if local_entries:
        (layout.directory / "metadata").mkdir(parents=True, exist_ok=True)
        _migrate_metadata(
            local_io,
            local_entries,
            f"file://{legacy_metadata}",
            f"file://{layout.directory / 'metadata'}",
            local_old,
            layout.table_location,
        )

    remote_entries: list[str] = []
    if archive and s3 is not None:
        prefix = archive.rstrip("/")
        remote_old = f"{prefix}/{NAMESPACE}/{name}"
        remote_entries = _entries(f"{remote_old}/metadata", s3)
        if remote_entries:
            remote_io = load_file_io(s3.resolved().catalog_properties(), prefix)
            _migrate_metadata(
                remote_io,
                remote_entries,
                f"{remote_old}/metadata",
                f"{layout.archive_table_location(prefix)}/metadata",
                remote_old,
                layout.archive_table_location(prefix),
            )

    _split_catalog(
        legacy_catalog,
        layout.catalog_db,
        LOCAL_CATALOG,
        name,
        (local_old, layout.table_location),
    )
    if legacy_archive.exists():
        prefix = (archive or "").rstrip("/")
        rewrite = (
            (f"{prefix}/{NAMESPACE}/{name}", layout.archive_table_location(prefix))
            if prefix
            else None
        )
        _split_catalog(
            legacy_archive, layout.archive_db, ARCHIVE_CATALOG, name, rewrite
        )

    # Verify BEFORE deleting anything. The old tree is still intact at this
    # point, so a mismatch here is recoverable by doing nothing.
    after = _pointer(layout.catalog_db, LOCAL_CATALOG, name)
    if expected is not None:
        actual = None if after is None else _records(local_io, after)
        if actual != expected:
            layout.catalog_db.unlink(missing_ok=True)
            layout.archive_db.unlink(missing_ok=True)
            msg = (
                f"migrated table reports {actual} rows where the original had "
                f"{expected}. Nothing was deleted and the new catalogs have "
                f"been removed; the log is untouched at {root}."
            )
            raise RuntimeError(msg)

        plan.notes.append(f"verified: {expected} rows before and after")

    for entry in local_entries:
        Path(entry.removeprefix("file://")).unlink(missing_ok=True)

    if legacy_metadata.exists():
        shutil.rmtree(legacy_metadata.parent, ignore_errors=True)
        with_namespace = root / NAMESPACE
        if with_namespace.is_dir() and not any(with_namespace.iterdir()):
            with_namespace.rmdir()

    if remote_entries and s3 is not None and archive:
        filesystem = _s3_filesystem(s3)
        # THIS STREAM's directory, never the namespace above it. Under the old
        # layout `<prefix>/litelink/` is SHARED: it holds the metadata of every
        # stream archived to this prefix, and sharing one was the ordinary
        # arrangement, because the catalogs and the WAL replica were shared
        # too. Sweeping the namespace deleted the manifests of every sibling
        # that had not migrated yet — and their data objects then survive with
        # nothing left to name them, since the offset-to-file mapping lived
        # only in those manifests. The local side has always been scoped this
        # way; the remote side lost it.
        namespace = f"{archive.rstrip('/')}/{NAMESPACE}"
        _delete_prefix(filesystem, f"{namespace}/{name}")

        # And the namespace itself only once nothing is left under it — the
        # same rule as the local `rmdir`.
        if not _prefix_holds(namespace, s3):
            _delete_prefix(filesystem, namespace)

    outstanding = _sibling_streams(legacy_catalog, name)
    if outstanding:
        plan.notes.append(
            f"{', '.join(sorted(outstanding))} still resolve through the shared "
            f"files at {root}, so `catalog.db`, `archive.db` and "
            f"`litestream.yml` stay until they migrate too. The old sidecar "
            f"keeps replicating them; leave it running"
        )
    else:
        legacy_catalog.unlink(missing_ok=True)
        legacy_archive.unlink(missing_ok=True)

        # The root config, and ONLY once no sibling depends on it. It is the
        # single per-root config naming every stream's buffer, so removing it
        # on the first stream's migration left every other stream with no
        # replication config anywhere — silently, at the sidecar's next
        # restart. While siblings remain it is not even stale: their layout has
        # not moved and it still names their buffers correctly.
        stale = root / "litestream.yml"
        if stale.exists():
            stale.unlink()
            plan.notes.append(
                f"removed {stale} — it named replica paths that no longer "
                f"exist. Regenerate per stream with "
                f"`litelink.open(root, name).write_replication_config()`, which "
                f"writes to <root>/<name>/litestream.yml, then restart the "
                f"sidecar for each"
            )

        for stem in ("catalog.db", "archive.db"):
            for suffix in ("-wal", "-shm"):
                (root / f"{stem}{suffix}").unlink(missing_ok=True)

            # litestream's local shadow directory for a database that has
            # moved. Regenerable, and stale where it is: the replica path
            # changed with the layout, so the sidecar starts a fresh chain.
            shutil.rmtree(root / f".{stem}-litestream", ignore_errors=True)

    (root / f"{name}-rewrite.db").unlink(missing_ok=True)

    # No drop here, and not by omission: `build_plan` refuses the flag whenever
    # the tree is still legacy, which is the only condition under which this
    # path runs. A call would be unreachable with `drop=True` and would read
    # like a live second site for an operation that must happen exactly once,
    # in the second pass. See the `not is_legacy` branch above.

    return plan


def _delete_prefix(filesystem: Any, location: str) -> int:
    """Remove everything under an S3 prefix. Returns how many objects there were.

    `delete_dir`, not a walk that unlinks each file. Object storage has no
    directories, but the stores that emulate them leave zero-byte `.../` keys
    behind — and pyarrow classifies those as directories, so a delete pass
    filtered on `is_file` walks straight past every one of them. Against rustfs
    that pass found nothing to do and reported success while all nine keys of a
    legacy replica stayed exactly where they were.
    """
    from pyarrow.fs import FileSelector

    selector = FileSelector(
        location.removeprefix("s3://"), recursive=True, allow_not_found=True
    )
    found = len(filesystem.get_file_info(selector))
    with contextlib.suppress(OSError):
        filesystem.delete_dir(location.removeprefix("s3://"))

    return found


def _legacy_wal_keys(root: Path, name: str, legacy: str) -> list[str]:
    """The keys under `<prefix>/_wal` that THIS root put there.

    Derived, never listed. `<prefix>/_wal` is shared per root, and nothing
    binds an archive prefix to a single root — 0.1's own advice for several
    logs was a root each, which makes two roots on one prefix an ordinary
    arrangement. Deleting the prefix wholesale therefore reaches another root's
    `<stream>/buffer.db`, and that replica holds unsealed rows which are in no
    Parquet file and no archive manifest.

    So: this stream's buffer replica, plus every sibling still named in the
    shared catalog, plus the two shared catalogs themselves. Anything else
    under `<prefix>/_wal` provably belongs to somebody else and is left alone.
    """
    streams = [name, *_sibling_streams(root / "catalog.db", name)]
    keys = [f"{legacy}/{stream}/buffer.db" for stream in streams]

    return [*keys, f"{legacy}/catalog.db", f"{legacy}/archive.db"]


def _prefix_holds(
    prefix: str, options: S3Options | None, *, only_files: bool = False
) -> bool:
    """Whether anything lives under an S3 prefix, at any depth.

    Both readings are wanted, in opposite places, which is why the filter is a
    parameter rather than two functions. Counting the zero-byte `.../` keys
    object stores leave behind answers "is this still listable", which decides
    whether there is tidying to do. Counting only real files answers "is this
    somebody's data", which decides whether the tidying is safe.
    """
    if options is None or not prefix.startswith("s3://"):
        return False

    from pyarrow.fs import FileSelector

    selector = FileSelector(
        prefix.removeprefix("s3://"), recursive=True, allow_not_found=True
    )
    found = _s3_filesystem(options).get_file_info(selector)

    return any(info.is_file for info in found) if only_files else bool(found)


def _drop_legacy_wal(
    plan: Plan,
    root: Path,
    name: str,
    archive: str | None,
    options: S3Options | None,
    *,
    drop: bool,
) -> None:
    """Remove this root's keys under `<prefix>/_wal`, the pre-0.2 replica.

    A second pass by design, run once the sidecar has established a fresh
    replica under `<prefix>/<name>/_wal`. `build_plan` refuses until it has —
    between the two there is a window with no current replica at all, and the
    old one is the only thing that could recover the log inside it.
    """
    if not drop or not archive or options is None:
        return

    legacy = f"{archive.rstrip('/')}/{WAL_PREFIX}"
    filesystem = _s3_filesystem(options)
    removed = 0
    for key in _legacy_wal_keys(root, name, legacy):
        removed += _delete_prefix(filesystem, key)

    # The intermediate `.../` markers, once no real file is left under the
    # prefix. Emptied of data it is still listable, so an operator checking
    # whether the old tree is gone is told it is not. Guarded on FILES rather
    # than on keys, so a prefix another root still replicates into keeps its
    # markers along with its data.
    if removed and not _prefix_holds(legacy, options, only_files=True):
        _delete_prefix(filesystem, legacy)

    plan.notes.append(
        f"dropped the legacy WAL replica this root owns under {legacy} "
        f"({removed} keys). Anything else there belongs to another root"
        if removed
        else f"nothing of this root's under {legacy} — already dropped"
    )


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m litelink.migrate --root DIR --name NAME [--apply]`.

    A dry run by default, and that is not politeness: this moves the files a
    log resolves through, against a bucket, and the shape of the tree is the
    one thing worth reading before it changes.
    """
    parser = argparse.ArgumentParser(
        prog="python -m litelink.migrate",
        description="Move a log to the per-stream layout (SPEC §2).",
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--archive", default=None, help="s3://bucket/prefix")
    parser.add_argument("--apply", action="store_true", help="default: dry run")
    parser.add_argument(
        "--drop-legacy-wal",
        action="store_true",
        help="remove <prefix>/_wal — only once a fresh replica has landed",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    # From the environment at the point of use, never from a file beside the
    # log: a log directory gets copied and must not carry a key with it.
    s3 = S3Options.from_env() if args.archive else None

    if args.apply:
        plan = migrate(
            args.root,
            args.name,
            archive=args.archive,
            s3=s3,
            drop_legacy_wal=args.drop_legacy_wal,
        )
    else:
        plan = build_plan(
            args.root,
            args.name,
            archive=args.archive,
            s3=s3,
            drop_legacy_wal=args.drop_legacy_wal,
        )
        if plan.needed:
            plan.notes.append("dry run — nothing was changed. Re-run with --apply")

    print(plan)  # noqa: T201

    return 1 if plan.blockers else 0
