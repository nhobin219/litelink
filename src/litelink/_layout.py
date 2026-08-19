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
    from datetime import date

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
    def table_id(self) -> str:
        return f"{NAMESPACE}.{self.name}"

    @property
    def data_dirs(self) -> tuple[Path, ...]:
        """Every directory this log may put a data file in.

        Its own seal output, and the warehouse directory pyiceberg writes into.
        Scoped to one log: anything wider would reach a sibling stream's files.
        """
        return (self.root / self.name, self.root / f"{NAMESPACE}.db" / self.name)

    def seal_path(self, start: int, end: int, day: date) -> str:
        """Root-relative path for a seal covering `[start, end)` (§4).

        The date is passed in rather than read from the clock, so the caller
        that persists this path is the one that chose it. Recomputing it later
        could land in a different day's directory and strand the first file.
        """
        return f"{self.name}/data/{day.isoformat()}/{start}-{end}.parquet"

    def compaction_path(self, lo: int, hi: int) -> str:
        """Root-relative path for the merge of the offset range `[lo, hi]` (§6)."""
        return f"{self.name}/data/compacted/{lo}-{hi}.parquet"

    def absolute(self, rel_path: str) -> Path:
        return self.root / rel_path

    def relative(self, path: str | Path) -> str:
        return str(Path(str(path).removeprefix("file://")).relative_to(self.root))

    def create(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / self.name).mkdir(parents=True, exist_ok=True)
