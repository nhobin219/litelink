"""The reader: watch a live log accumulate.

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


def snapshot(log: Log) -> tuple[int, int, int, int]:
    """(total rows, table rows, buffer rows, data files) — one read, one moment."""
    extent = log.table_extent()
    total = log.scan(columns=["litelink_offset"]).read_all().num_rows
    table = (
        0
        if extent is None
        else log.scan(columns=["litelink_offset"], end_offset=extent[1] + 1)
        .read_all()
        .num_rows
    )

    return total, table, total - table, len(log._table.data_files())  # noqa: SLF001


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument(
        "--every", type=float, default=2.0, help="seconds between reads"
    )
    args = parser.parse_args()

    log = Log.open(args.root, NAME, read_only=True)
    print(f"tailing {args.root}/{NAME} (readonly). Ctrl-C to stop.\n")
    print(f"{'total':>12} {'in table':>12} {'in buffer':>12} {'files':>7} {'read':>8}")

    previous = 0
    try:
        while True:
            started = time.monotonic()
            total, table, buffered, files = snapshot(log)
            elapsed_ms = (time.monotonic() - started) * 1000

            delta = f"+{total - previous:,}" if total > previous else ""
            print(
                f"{total:>12,} {table:>12,} {buffered:>12,} {files:>7} "
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
