"""The writer: append continuously, maintain in the background.

    uv run python examples/capture.py [--root DIR] [--rate ROWS_PER_SECOND]

Two threads, which is the whole operational shape of a litelink process:

  - the main thread appends, and every append is durable when it returns
  - a daemon thread calls maintain() on an interval, reclaiming disk

They share one Log because SQLite serialises the writes, and §1's single-writer
rule is about processes, not threads. Ctrl-C to stop; nothing committed is lost,
because there is no in-memory buffer to lose.
"""

from __future__ import annotations

import argparse
import itertools
import threading
import time
from datetime import timedelta
from pathlib import Path

from _stream import NAME, SCHEMA, SORT_BY, observations

from litelink import Log, LogConfig


def maintain_forever(log: Log, every: float, stop: threading.Event) -> None:
    while not stop.wait(every):
        try:
            log.maintain()
        except Exception as exc:  # noqa: BLE001 - a daemon must not die quietly
            print(f"  [maintain] failed: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument("--rate", type=float, default=200.0, help="rows per second")
    parser.add_argument("--batch", type=int, default=20, help="rows per transaction")
    parser.add_argument("--maintain-every", type=float, default=10.0, help="seconds")
    args = parser.parse_args()

    # Deliberately small for a demo: seal often so the reader sees the table
    # grow within seconds. A real deployment sizes this by §7's read-latency
    # argument, not to make a demo lively.
    config = LogConfig(
        target_size=256 * 1024,
        compact_below=1024 * 1024,
        compact_min_files=3,
        snapshot_retention=timedelta(seconds=30),
    )

    log = Log(args.root, NAME, schema=SCHEMA, sort_by=SORT_BY, config=config)
    stop = threading.Event()
    maintainer = threading.Thread(
        target=maintain_forever, args=(log, args.maintain_every, stop), daemon=True
    )
    maintainer.start()

    print(f"capturing into {args.root}/{NAME} at ~{args.rate:.0f} rows/s")
    print(f"maintain() every {args.maintain_every:.0f}s in a daemon thread")
    print("run examples/tail.py in another terminal. Ctrl-C to stop.\n")

    source = observations()
    interval = args.batch / args.rate
    written = 0
    started = time.monotonic()

    try:
        for tick in itertools.count():
            deadline = started + (tick + 1) * interval
            log.extend(itertools.islice(source, args.batch))
            written += args.batch

            if tick % 50 == 0:
                elapsed = time.monotonic() - started
                print(
                    f"  {written:>9,} rows  {written / max(elapsed, 1e-9):>7,.0f}/s"
                    f"  end_offset={log.end_offset():,}"
                )

            # Pace against a fixed schedule rather than sleeping a fixed amount,
            # so a slow batch does not permanently lower the rate.
            time.sleep(max(0.0, deadline - time.monotonic()))
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        stop.set()
        log.close()

    print(f"{written:,} rows appended, all durable at commit")


if __name__ == "__main__":
    main()
