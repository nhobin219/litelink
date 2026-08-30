"""The reader: watch a live position feed accumulate.

    uv run python examples/adsb/tail.py [--root DIR] [--every SECONDS]

Opens the same log readonly while examples/adsb/capture.py writes it, and prints
where the rows are. The interesting column is the split: rows move from the
buffer into the Iceberg table at each seal, and the total never double-counts
across that boundary because both legs are derived from one committed extent
(SPEC §7, I3).
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from _stream import NAME

import litelink
from litelink import LogHandle
from litelink._s3 import S3Options


def snapshot(log: LogHandle) -> tuple[int, int, int, int, int]:
    """(stream rows, local rows, buffer rows, local files, archived rows).

    Nothing here scans data. Iceberg tracks a row count per file, so the table's
    total comes off the manifest read that produced the boundary; the buffer is
    counted in SQLite as a rowid range above it.

    The difference is the shape, not the constant. A `count(*)` over the log
    reads the offset column out of every Parquet file, so the poll got slower as
    the log grew — 24 ms at 50,000 rows and climbing. This is flat.

    Everything here is local and free, which is what lets the `read` column
    mean something. The archive's file count is neither, so it is not in here —
    see `ArchiveCount`.

    With an archive, `table + buffer` stops being the whole stream: eviction
    removes files the archive has, so the local tiers SHRINK while the stream
    only grows. `end_offset` is the count that keeps growing either way — it is
    the next offset to be assigned, so one less than it is every row ever
    appended, whichever tier now holds it. The watermark says how much of that
    the archive has taken, and the gap between them is how far sync is behind.
    """
    return (
        log.end_offset() - 1,
        log.table_rows(),
        log.buffered_rows(),
        log.table_files(),
        log.archived_through(),
    )


class ScanCost:
    """Time to read every row, measured on a cadence rather than every tick.

    The `read` column beside this is deliberately flat — `snapshot()` answers
    from manifest statistics and a rowid range, touching no data — and that is
    the number to watch for latency. It is also not the number anyone means by
    "how fast is it": a full scan is, and its cost GROWS with the log, which is
    the contrast worth showing.

    Not every tick, because a full scan of a growing log is unbounded work and
    would swamp a two-second poll. The last measurement is held between runs,
    so the column always shows a real number rather than blanks — and the
    header says how old it may be, or a stale value reads as a fresh one.
    """

    def __init__(self, every: float) -> None:
        self._every = every
        self._at = 0.0
        self.rows = 0
        self.seconds = 0.0

    def due(self) -> bool:
        return time.monotonic() - self._at >= self._every

    def measure(self, log: LogHandle, *, archived: bool) -> None:
        self._at = time.monotonic()
        started = time.monotonic()
        # `read_all`, not a count: a count is answered from statistics without
        # opening a data file, which would measure the wrong thing entirely.
        self.rows = log.scan(include_archive=archived).read_all().num_rows
        self.seconds = time.monotonic() - started

    def rate(self) -> str:
        """Rows per second, in a unit that suits the number.

        Fixed at millions, a demo of a few thousand rows reads `0.1M/s` for
        every scan it ever does — the column stops distinguishing anything,
        which is the one thing it is there for.
        """
        if not self.seconds or not self.rows:
            return ""

        per_second = self.rows / self.seconds
        if per_second >= 1e6:
            return f"{per_second / 1e6:.1f}M/s"

        return f"{per_second / 1e3:.0f}k/s"


class ArchiveCount:
    """How many files the archive holds, fetched only when it can have changed.

    Counting what object storage holds means asking object storage, so this is
    the one number in the display that is not free — asking every tick took the
    read from 8 ms to 300 ms and buried the local latency the `read` column is
    there to show.

    The watermark is the signal. It is a local read, and it moves exactly when
    `sync` commits, which is exactly when the archive can have gained files. So
    a tick with nothing new to report costs no round trip, and one that does
    pays for it once.
    """

    def __init__(self) -> None:
        self._watermark = -1
        self._files = 0

    def at(self, log: LogHandle, watermark: int) -> int:
        if watermark != self._watermark:
            self._watermark = watermark
            self._files = log.archive_files()

        return self._files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument(
        "--every", type=float, default=2.0, help="seconds between reads"
    )
    parser.add_argument(
        "--scan-every",
        type=float,
        default=30.0,
        help="seconds between full scans; 0 disables them",
    )
    args = parser.parse_args()

    log = litelink.open(args.root, NAME, read_only=True, s3=S3Options())
    print(f"tailing {args.root}/{NAME} (readonly). Ctrl-C to stop.")
    if log.archive:
        print(f"archive: {log.archive}")
        print("local falls as archived rises; stream counts every row either way")

    print()
    # Named for what they are rather than for where they live. "in table"
    # meant local rows and "files" meant local files, which asks the reader to
    # remember the implementation to read the display.
    archived_rows = f" {'archived rows':>14}" if log.archive else ""
    archived_files = f" {'archived files':>14}" if log.archive else ""
    scanning = args.scan_every > 0
    scan_header = f" {'scan':>10} {'rate':>8}" if scanning else ""
    print(
        f"{'stream rows':>13} {'buffer rows':>13} {'local rows':>13}{archived_rows}"
        f" {'local files':>12}{archived_files} {'read':>9}{scan_header}"
    )
    if scanning:
        # Said once, because the two timings mean different things and the
        # column headers cannot carry it: `read` is this tick, `scan` is up to
        # `--scan-every` seconds old.
        print(
            f"  read = metadata only, every tick; "
            f"scan = every row, every {args.scan_every:g}s"
        )

    previous = 0
    archive = ArchiveCount()
    scan = ScanCost(args.scan_every)
    try:
        while True:
            started = time.monotonic()
            total, table, buffered, files, archived = snapshot(log)
            remote = archive.at(log, archived) if log.archive else 0
            elapsed_ms = (time.monotonic() - started) * 1000

            if scanning and scan.due():
                scan.measure(log, archived=bool(log.archive))

            delta = f"+{total - previous:,}" if total > previous else ""
            measured = (
                f" {scan.seconds * 1000:>8.0f}ms {scan.rate():>8}" if scanning else ""
            )
            print(
                f"{total:>13,} {buffered:>13,} {table:>13,}"
                f"{f' {archived:>14,}' if log.archive else ''}"
                f" {files:>12,}{f' {remote:>14,}' if log.archive else ''}"
                f" {elapsed_ms:>7.1f}ms{measured}  {delta}"
            )
            previous = total
            time.sleep(args.every)
    except KeyboardInterrupt:
        print("\nstopped")
    except RuntimeError as exc:
        # Ctrl-C landing inside a DuckDB call comes back as
        # RuntimeError("Query interrupted") rather than KeyboardInterrupt —
        # DuckDB unwinds its own execution first. Matched narrowly rather than
        # catching RuntimeError outright, which would swallow a real failure.
        if "interrupted" not in str(exc).lower():
            raise

        print("\nstopped")
    finally:
        log.close()


if __name__ == "__main__":
    main()
