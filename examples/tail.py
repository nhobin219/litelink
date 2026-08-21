"""The reader: watch a live position feed accumulate.

    uv run python examples/tail.py [--root DIR] [--every SECONDS]

Opens the same log readonly while examples/capture.py writes it, and prints
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

from litelink import Log
from litelink._s3 import S3Options


def snapshot(log: Log, remote: tuple[int, int]) -> tuple[int, int, int, int, int, int]:
    """(stream, table rows, buffer rows, local files, archived, remote files).

    Nothing here scans data. Iceberg tracks a row count per file, so the table's
    total comes off the manifest read that produced the boundary; the buffer is
    counted in SQLite as a rowid range above it.

    The difference is the shape, not the constant. A `count(*)` over the log
    reads the offset column out of every Parquet file, so the poll got slower as
    the log grew — 24 ms at 50,000 rows and climbing. This is flat.

    The archive file count is the exception, so it is refreshed only when the
    watermark moves. That local number changes exactly when `sync` commits,
    which is exactly when the archive can have gained files — so the signal is
    free, and a tick that has nothing new to report costs no round trip. Asking
    every tick took the read from 8 ms to 300 ms and buried the local latency
    this is here to show.

    With an archive, `table + buffer` stops being the whole stream: eviction
    removes files the archive has, so the local tiers SHRINK while the stream
    only grows. `end_offset` is the count that keeps growing either way — it is
    the next offset to be assigned, so one less than it is every row ever
    appended, whichever tier now holds it. The watermark says how much of that
    the archive has taken, and the gap between them is how far sync is behind.
    """
    archived = log.archived_through()
    seen, files = remote

    return (
        log.end_offset() - 1,
        log.table_rows(),
        log.buffered_rows(),
        log.table_files(),
        archived,
        files if archived == seen else log.archive_files(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument(
        "--every", type=float, default=2.0, help="seconds between reads"
    )
    args = parser.parse_args()

    log = Log.open(args.root, NAME, read_only=True, s3=S3Options())
    print(f"tailing {args.root}/{NAME} (readonly). Ctrl-C to stop.")
    if log.archive:
        print(f"archive: {log.archive}")
        print("`files` is local disk, `arch files` is object storage — the first")
        print("falls as the second rises, and `stream` counts every row either way")

    print()
    archived_column = f" {'archived':>12} {'arch files':>10}" if log.archive else ""
    print(
        f"{'stream':>12} {'in table':>12} {'in buffer':>12}"
        f"{archived_column} {'files':>7} {'read':>8}"
    )

    previous = 0
    # (watermark the count was taken at, the count) — see `snapshot`.
    remote_at = (-1, 0)
    try:
        while True:
            started = time.monotonic()
            total, table, buffered, files, archived, remote = snapshot(log, remote_at)
            remote_at = (archived, remote)
            elapsed_ms = (time.monotonic() - started) * 1000

            delta = f"+{total - previous:,}" if total > previous else ""
            print(
                f"{total:>12,} {table:>12,} {buffered:>12,}"
                f"{f' {archived:>12,} {remote:>10,}' if log.archive else ''}"
                f" {files:>7} "
                f"{elapsed_ms:>7.1f}ms  {delta}"
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
