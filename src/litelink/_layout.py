"""Where a log's files live (SPEC §2, §4).

Pure path derivation, no I/O beyond creating the directories. Isolated because
these names are load-bearing in two directions: a seal's path is claimed in
SQLite before the file exists (I2), and reclamation is a keyed read of those
same paths rather than a directory scan.
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
    def buffer_db(self) -> Path:
        """One SQLite database per stream — SQLite's write lock is per file."""
        return self.root / self.name / "buffer.db"

    @property
    def catalog_db(self) -> Path:
        """The catalog is a file, not a service. Shared by every log under root."""
        return self.root / "catalog.db"

    @property
    def catalog_uri(self) -> str:
        return f"sqlite:///{self.catalog_db}"

    @property
    def warehouse_uri(self) -> str:
        return f"file://{self.root}"

    @property
    def archive_db(self) -> Path:
        """The archive catalog, kept beside the local one (§2).

        A local SQLite file describing a warehouse on object storage. §2
        replicates this file to S3 as well, so another machine can attach to
        the archive without this one; that replication is not built yet.
        """
        return self.root / "archive.db"

    @property
    def archive_catalog_uri(self) -> str:
        return f"sqlite:///{self.archive_db}"

    def archive_key(self, rel_path: str) -> str:
        """Where a local file lands in the archive prefix.

        The same root-relative name under the archive's prefix, so a file's
        identity is its offset range in both tiers and neither has to translate
        the other's paths.
        """
        return rel_path

    @property
    def table_id(self) -> str:
        return f"{NAMESPACE}.{self.name}"

    @property
    def data_dirs(self) -> tuple[Path, ...]:
        """Every directory this log may put a data file in.

        Its own seal output, and the warehouse directory pyiceberg writes into.
        Scoped to one log: anything wider would reach a sibling stream's files.
        """
        return (self.root / self.name, self.root / f"{NAMESPACE}.db" / self.name)

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
        (self.root / self.name).mkdir(parents=True, exist_ok=True)
