"""The writer: append, and nothing else.

    uv run python examples/capture.py [--root DIR] [--rate ROWS_PER_SECOND]
    uv run python examples/capture.py --no-seal    # alongside examples/sealer.py

One thread appending, and every append durable when `extend()` returns — there
is no in-memory buffer to flush, which is the failure the README opens with.

**Sealing and maintenance are separate processes**, in `sealer.py` and
`maintainer.py`. They are separate from each other too: different roles, holding
different leases, so either can be restarted or crash without stopping the
other, and a compaction that takes seconds cannot delay a seal.

By default this still seals on a background thread, so the demo does something
on its own. Start `examples/sealer.py` and that thread begins losing the lease
immediately — no restart here, no flag, no coordination. `--no-seal` skips
starting a thread that would only lose, which is what a real deployment wants
once it runs a dedicated sealer.

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
    parser.add_argument(
        "--no-seal",
        action="store_true",
        help="do not seal here; run examples/sealer.py instead",
    )
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
        # than the target would sit in SQLite indefinitely.
        max_age=timedelta(seconds=15),
        seal_mode="none" if args.no_seal else "background",
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
    if args.no_seal:
        print("not sealing here — run `just demo-seal`")
    else:
        print("sealing on a background thread until `just demo-seal` takes over")

    print("`just demo-maintain` reclaims disk; `just demo-tail` watches. Ctrl-C.\n")

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
