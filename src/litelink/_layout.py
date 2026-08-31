"""Where a log's files live (SPEC §2, §4).

Pure path derivation, no I/O beyond creating the directories. Isolated because
these names are load-bearing in two directions: a seal's path is claimed in
SQLite before the file exists (I2), and reclamation is a keyed read of those
same paths rather than a directory scan.

**One directory per stream, holding everything that stream owns.** Every path
below is under `<root>/<name>`, so a log is a subtree that can be copied,
replicated or deleted whole. It was not always so: `catalog.db` and
`archive.db` used to sit at the root and be shared by every log under it,
which bought nothing — every query against them is keyed by
`(catalog, namespace, table)` and no code has ever read across streams — and
cost three things. A sidecar per log would have run two litestream instances
against one `catalog.db`, which litestream forbids, so replication was
one-sidecar-per-root with a config that had to be written by hand. `follow`
had to drop its `root` parameter, because a caller-supplied directory could
collide with a live log's shared catalogs. And one corrupt catalog took every
stream under the root with it.

Contention was NOT among the costs, which is worth recording because it is the
first thing anyone assumes: two streams sealing concurrently against one
shared `catalog.db` measured a 57.3 ms median against 66.1 ms for separate
roots, over 16 seals. The Iceberg commit is a tiny transaction; a seal's
seconds go to the Parquet and Avro writes, which were always per-stream.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

NAMESPACE = "litelink"


@dataclass(frozen=True, slots=True)
class Layout:
    """The file and catalog layout of one log under one root."""

    root: Path
    name: str

    def __post_init__(self) -> None:
        """Force `root` absolute.

        Both URIs below are broken by a relative path, in different ways.
        `file://litelink-data/x` is not a relative file URI — it parses as host
        `litelink-data` and path `/x`, and DuckDB reports a missing file naming
        a path that plainly exists. `sqlite:///litelink-data/catalog.db` does
        work, but only relative to the process's cwd, so the same log resolves
        to different databases depending on where it was opened from.

        Resolved here rather than at each call site because the URIs are
        properties: anything that constructs a Layout gets it, and there is no
        way to hold one that is relative.
        """
        object.__setattr__(self, "root", Path(self.root).resolve())

    @property
    def directory(self) -> Path:
        """Everything this stream owns, and nothing another stream does.

        The unit of replication, restore and deletion. `data/` and `metadata/`
        sit under it because that is where Iceberg puts them once the table
        location is this directory, and the three SQLite files sit beside them
        because they describe this stream alone.
        """
        return self.root / self.name

    @property
    def buffer_db(self) -> Path:
        """One SQLite database per stream — SQLite's write lock is per file."""
        return self.directory / "buffer.db"

    @property
    def catalog_db(self) -> Path:
        """The catalog is a file, not a service. One per stream (§2)."""
        return self.directory / "catalog.db"

    @property
    def legacy_catalog_db(self) -> Path:
        """Where `catalog.db` sat before 0.2 — shared by every log under root.

        Kept as a derived path rather than a literal at each call site, because
        several places have to distinguish "no log here" from "a log here that
        has not been migrated", and answering that wrong is destructive: see
        `LogTable.exists_for`.
        """
        return self.root / "catalog.db"

    def is_legacy(self) -> bool:
        """Whether this log still keeps its catalogs at the root (pre-0.2).

        Both halves matter. A root-level `catalog.db` alone could be a sibling
        stream's leftover; what says THIS log has not moved is that it has none
        of its own.
        """
        return self.legacy_catalog_db.exists() and not self.catalog_db.exists()

    @property
    def catalog_uri(self) -> str:
        return f"sqlite:///{self.catalog_db}"

    @property
    def warehouse_uri(self) -> str:
        """Still the root, because `relative` is what turns a local file into
        an archive key and both tiers have to agree on it.

        The table's location is passed explicitly at creation rather than left
        to `<warehouse>/<namespace>/<table>`, so this no longer decides where
        metadata lands. See `table_location`.
        """
        return f"file://{self.root}"

    @property
    def table_location(self) -> str:
        """Where the local Iceberg table lives — data AND metadata (§2).

        Passed to `create_table` explicitly, which is the whole point. Left to
        pyiceberg it resolved to `<warehouse>/<namespace>/<table>`, so metadata
        landed in `<root>/litelink/<name>/metadata` while `seal_path` wrote
        data to `<root>/<name>/data`: a table whose data files were outside its
        own location, held together only by the absolute paths in its
        manifests. Deleting the table directory would have left every Parquet
        file behind.
        """
        return f"file://{self.directory}"

    def archive_table_location(self, prefix: str) -> str:
        """The same, in the archive prefix.

        A data file keeps its root-relative name in the archive — `LogTable`
        maps `<name>/data/...` to `<prefix>/<name>/data/...` — so the remote
        table's location has to be `<prefix>/<name>` for its metadata to sit
        beside its data the way the local one now does.
        """
        return f"{prefix.rstrip('/')}/{self.name}"

    @property
    def replication_config(self) -> Path:
        """Where `write_replication_config` puts the sidecar's config.

        Inside the stream's directory, because there is now one sidecar per
        stream: all three databases are here, and `destination` sends them to
        `<prefix>/<name>/_wal`. At the root — where this used to be — two logs
        would write one file and the second would silently stop replicating the
        first.
        """
        return self.directory / "litestream.yml"

    @property
    def rewrite_db(self) -> Path:
        """Scratch buffer for an archive rewrite.

        Its own file, not the log's: the rewrite re-ingests archived rows
        through an ordinary `Buffer` to re-cut them, and that buffer must not
        be the log's own — appends are still landing there, and its offsets are
        the live ones. Inside the stream directory like everything else, so a
        crash leaves the strays where the stream's own cleanup can see them.
        """
        return self.directory / "rewrite.db"

    @property
    def archive_db(self) -> Path:
        """The archive catalog, kept beside the local one (§2).

        A local SQLite file describing a warehouse on object storage. It is
        replicated like the others — but it is deliberately NOT restored onto
        another machine. Its paths are `s3://` and so machine-independent, yet
        it is TIME-dependent, and a stale copy is worse than none:
        `open_archive` consults `version-hint.text` only when the catalog has
        no row, so a stale row wins over the bucket's own pointer and the
        archive reads short, silently. See `litelink.restore`.
        """
        return self.directory / "archive.db"

    @property
    def archive_catalog_uri(self) -> str:
        return f"sqlite:///{self.archive_db}"

    @property
    def databases(self) -> tuple[Path, ...]:
        """Every SQLite file a restore needs, in dependency order (§3a).

        What a WAL-shipping sidecar has to replicate. All three, not just the
        buffer: the buffer holds rows no Parquet file has yet, `catalog.db`
        holds which files the local table is made of, and `archive.db` holds
        the same for the archive.

        That last one used to be justified as the only thing able to name the
        objects in S3. It is not, since the archive publishes
        `version-hint.text`; it is replicated for the SAME-machine case, where
        it saves a round trip, and a failover deliberately does not restore it
        because a stale copy wins over the bucket's own pointer.

        The rewrite scratch is excluded. It is derived from the archive and
        deleted at the end of the operation that makes it, so replicating it
        would ship a temporary file to object storage to no purpose.

        Listed here rather than assembled by a caller, because which files
        matter is exactly what this class knows and nothing else should have to
        rediscover by listing a directory.
        """
        return (self.buffer_db, self.catalog_db, self.archive_db)

    @property
    def table_id(self) -> str:
        """The catalog identifier, which is NOT a path.

        The namespace survives the move to per-stream directories on purpose:
        it names the table inside its catalog, and `table_location` decides
        where the bytes go. Keeping them separate is what let the layout change
        without rewriting a single catalog row's identity.
        """
        return f"{NAMESPACE}.{self.name}"

    def seal_path(self, start: int, end: int, token: str) -> str:
        """Root-relative path for a seal covering `[start, end)` (§4).

        `token` makes it unique per ATTEMPT, not per range. The name was once
        derived from the range alone, on the reasoning that a retry should
        overwrite in place and strand nothing — but recovery never recomputes
        it, it reads it back from `sealing`, so determinism bought nothing and
        cost the one thing it appeared to prevent. A writer stalled past its
        lease and the owner that took over both wrote that single name, and
        `pq.write_table` truncates on open, so the file became a blend of two
        writers with one of them committing it.

        Unique names alone would trade that for an untracked file, which is
        worse. They come with the rule that a superseded attempt is queued in
        `pending_delete` before its claim is replaced — see `_recover_seal`.

        No date directory. Seals used to be grouped by the day they were
        written, which nothing read: the table is unpartitioned, Iceberg finds
        files by the path in its manifests, and no code here ever lists a
        directory — that refusal is the whole reason `pending_delete` exists.
        What the grouping did produce was a way to strand a file, by
        recomputing a path across midnight and landing somewhere else.
        Compaction outputs were never dated, which is the tell.
        """
        return f"{self.name}/data/{start}-{end}-{token}.parquet"

    def compaction_path(self, lo: int, hi: int, token: str) -> str:
        """Root-relative path for the merge of the offset range `[lo, hi]` (§6).

        `token` makes it unique per attempt, and unlike a seal's path it does
        NOT need to be derivable: `compacting` records it before the file
        exists, so recovery reads the name rather than recomputing it.

        Uniqueness is the point. A deterministic `{lo}-{hi}` meant a compaction
        whose inputs were themselves a previous compaction of the same range
        wrote to the path it was reading — `set_sort_by(rewrite=True)` after
        any compaction truncated the live, table-referenced file, and a crash
        mid-write destroyed the only copy of those rows. It also meant two
        owners racing the role wrote one file. A seal can overwrite in place
        because its source is the buffer, which is still there; a compaction's
        source is the file it is replacing.
        """
        return f"{self.name}/data/compacted/{lo}-{hi}-{token}.parquet"

    def absolute(self, rel_path: str) -> Path:
        return self.root / rel_path

    def relative(self, path: str | Path) -> str:
        return str(Path(str(path).removeprefix("file://")).relative_to(self.root))

    def create(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.directory.mkdir(parents=True, exist_ok=True)
