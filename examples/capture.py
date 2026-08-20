"""The writer: append, and nothing else.

    uv run python examples/capture.py [--root DIR] [--rate ROWS_PER_SECOND]

One thread appending, and every append durable when `extend()` returns — there
is no in-memory buffer to flush, which is the failure the README opens with.

**Everything else is `maintainer.py`**, a separate process: sealing the buffer
into Parquet, then compacting, evicting and expiring what that produces.
Sealing is maintenance, not a third role — it is the first thing done with what
this process leaves behind.

`seal_mode="none"`, with no option to change it, because there is nothing to
decide: this process appends. Run it alone and the rows stay in SQLite, durable
and readable — `scan()` unions the buffer with the table, so a reader sees them
whether or not anything has sealed yet. They reach Parquet when the maintainer
starts, at exactly the cuts recorded while it was not running. Only the buffer
grows in the meantime, which is worth seeing.

Ctrl-C to stop. Nothing committed is lost, and nothing queued is either: a cut
that has been recorded but not yet sealed is picked up by whoever opens the log
next.
"""

from __future__ import annotations

import argparse
import itertools
import time
from datetime import timedelta
from pathlib import Path

from _stream import NAME, SCHEMA, SORT_BY, observations

from litelink import Log, LogConfig


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("litelink-data"))
    parser.add_argument("--rate", type=float, default=2000.0, help="reports per second")
    parser.add_argument("--batch", type=int, default=20, help="rows per transaction")
    args = parser.parse_args()

    # Deliberately small for a demo: seal often so the reader sees the table
    # grow within seconds. A real deployment sizes this by §7's read-latency
    # argument, not to make a demo lively.
    config = LogConfig(
        target_size=256 * 1024,
        compact_below=1024 * 1024,
        compact_min_files=3,
        snapshot_retention=timedelta(seconds=30),
        # Seal a quiet stream on a timer as well as by size, or a feed slower
        # than the target would sit in SQLite indefinitely. Read by whoever
        # holds the seal lease, which is never this process.
        max_age=timedelta(seconds=15),
        seal_mode="none",
    )

    # new() creates and takes the shape; open() recovers it. A service that
    # restarts wants the second, and must not fail because the log is already
    # there — so the choice is made by whether it exists.
    if (args.root / "catalog.db").exists():
        log = Log.open(args.root, NAME)
        log.set_config(config)
    else:
        log = Log.new(args.root, NAME, schema=SCHEMA, sort_by=SORT_BY, config=config)

    print(f"capturing {NAME} into {args.root} at ~{args.rate:,.0f} reports/s")
    print("appending only — `just demo-maintain` seals and reclaims disk")
    print("`just demo-tail` watches. Until a maintainer runs, rows stay buffered")
    print("and readable; nothing is lost by starting it late.\n")

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
                    f"  {written:>9,} reports  {written / max(elapsed, 1e-9):>7,.0f}/s"
                    f"  end_offset={log.end_offset():,}"
                )

            # Pace against a fixed schedule rather than sleeping a fixed amount,
            # so a slow batch does not permanently lower the rate.
            time.sleep(max(0.0, deadline - time.monotonic()))
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        log.close()

    on_disk = sum(f.stat().st_size for f in args.root.rglob("*") if f.is_file())
    print(f"{written:,} reports appended, all durable at commit")
    print(f"{args.root}/ holds {on_disk / 1e6:.1f} MB — `just demo-clean` to remove it")
    # Nothing deletes this on exit, deliberately: tail.py reads it after the
    # writer stops, and a demo you cannot inspect afterwards is not much of one.
    # Note the demo leaves local_retention unset, so the window grows without
    # bound; a real deployment sets it and lets maintain() hold the size.
    # Rows still queued or buffered here are not lost — they are durable, and
    # the next process to open the log finds the cuts already recorded.


if __name__ == "__main__":
    main()
